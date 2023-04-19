import datetime
import os.path
import time
from json import loads

from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.work_spec import WorkSpec
from pandaharvester.harvestercore.worker_errors import WorkerErrors
from pandaharvester.harvestercore.plugin_base import PluginBase
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestermisc import dask_utils

# logger
base_logger = core_utils.setup_logger('dask_monitor')

BAD_CONTAINER_STATES = ['CreateContainerError', 'CrashLoopBackOff', "FailedMount"]

# monitor for dask
class DaskMonitor(PluginBase):

    _tmpdir = os.environ.get('DASK_TMPDIR', '/tmp/panda')

    # constructor
    def __init__(self, **kwarg):
        PluginBase.__init__(self, **kwarg)

        self.panda_queues_dict = PandaQueuesDict()

        # retrieve the k8s namespace from CRIC
        #namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)
        #self.k8s_client = k8s_Client(namespace=namespace, queue_name=self.queueName, config_file=self.k8s_config_file)

        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 4
        try:
            self.cancelUnknown
        except AttributeError:
            self.cancelUnknown = False
        else:
            self.cancelUnknown = bool(self.cancelUnknown)
        try:
            self.podQueueTimeLimit
        except AttributeError:
            self.podQueueTimeLimit = 15 * 60  #172800

        self._all_pods_list = []

    def get_pod_info(self, podname, namespace):
        """
        Get the pod_info dictionary containing all the known pod information.
        """

        tmp_log = self.make_logger(base_logger, 'queueName={0}'.format(self.queueName),
                                   method_name='get_pod_info')

        pod_info = {}
        cmd = f'kubectl get pod {podname} --output=json -n {namespace}'
        ec, stdout, stderr = dask_utils.execute(cmd)
        if not ec and stdout:
            _dict = loads(stdout)

            try:
                pod_info['start_time'] = _dict['status']['startTime']
                pod_info['status'] = _dict['status']['startTime']
                pod_info['status_conditions'] = _dict['status']['conditions']
                pod_info['containers_state'] = []
                if _dict['status']['containerStatuses']:
                    for _cs in _dict['status']['containerStatuses']:
                        if _cs['state']:
                            pod_info['containers_state'].append(_cs['state'])
            except Exception as exc:
                tmp_log.warning(f'caught exception: {exc}')
        else:
            if 'not found' in stderr:
                tmp_log.debug(f'pilot pod not found: {stderr}')
                pass  # ignore since pod has not started yet
            else:
                tmp_log.warning(f'command (cmd) failed: {stderr}')

        return pod_info

    def get_pilot_exit_code(self, pod_info, state='unknown'):
        """
        Get the pilot container exit code.
        Note: a status Boolean is also returned to make sure that the exit code was correctly extracted.
        """
        # pod_info['containers_state'][0]['terminated']['exitCode']

        status = False
        tmp_log = self.make_logger(base_logger, 'queueName={0}'.format(self.queueName),
                                   method_name='get_pilot_exit_code')
        exit_code = 0
        if state not in pod_info['containers_state'][0]:
            tmp_log.warning(f'state {state} not found in pod_info={pod_info}')
        else:
            try:
                exit_code = pod_info['containers_state'][0][state]['exitCode']
            except Exception as exc:
                tmp_log.warning(f'caught exception: {exc}')
            else:
                status = True if isinstance(exit_code, int) else False

        return exit_code, status

    def check_pods_status(self, pods_status_list, containers_state_list):
        sub_msg = ''

        if 'Unknown' in pods_status_list:
            if all(item == 'Unknown' for item in pods_status_list):
                new_status = None
            elif 'Running' in pods_status_list:
                new_status = WorkSpec.ST_running
            else:
                new_status = WorkSpec.ST_idle

        else:
            # Pod in Pending status
            if all(item == 'Pending' for item in pods_status_list):
                new_status = WorkSpec.ST_submitted  # default is submitted, but consider certain cases
                for item in containers_state_list:
                    if item.waiting and item.waiting.reason in BAD_CONTAINER_STATES:
                        new_status = WorkSpec.ST_failed  # change state to failed

            # Pod in Succeeded status
            elif 'Succeeded' in pods_status_list:
                if all((item.terminated is not None and item.terminated.reason == 'Completed') for item in containers_state_list):
                    new_status = WorkSpec.ST_finished
                else:
                    sub_mesg_list = []
                    for item in containers_state_list:
                        msg_str = ''
                        if item.terminated is None:
                            state = 'UNKNOWN'
                            if item.running is not None:
                                state = 'running'
                            elif item.waiting is not None:
                                state = 'waiting'
                            msg_str = 'container not terminated yet ({0}) while pod Succeeded'.format(state)
                        elif item.terminated.reason != 'Completed':
                            msg_str = 'container terminated by k8s for reason {0}'.format(item.terminated.reason)
                        sub_mesg_list.append(msg_str)
                    sub_msg = ';'.join(sub_mesg_list)
                    new_status = WorkSpec.ST_cancelled

            # Pod in Running status
            elif 'Running' in pods_status_list:
                new_status = WorkSpec.ST_running

            # Pod in Failed status
            elif 'Failed' in pods_status_list:
                new_status = WorkSpec.ST_failed

            else:
                new_status = WorkSpec.ST_idle

        return new_status, sub_msg

    def check_a_worker(self, workspec):
        # set logger
        tmp_log = self.make_logger(base_logger, 'queueName={0} workerID={1} batchID={2}'.
                                   format(self.queueName, workspec.workerID, workspec.batchID),
                                   method_name='check_a_worker')

        # initialization
        status = None
        job_id = workspec.batchID
        err_str = ''
        time_now = datetime.datetime.utcnow()
        pods_status_list = []
        pods_name_to_delete_list = []
        tmp_log.debug('called check_a_worker()')
        # extract the namespace, scheduler and session pod names from the encoded workspec.namespace
        if workspec.namespace:
            _namespace, _taskid, _mode, _scheduler_pod_name, _session_pod_name, _pilot_pod_name = dask_utils.extract_pod_info(workspec.namespace)
        else:
            err_str = 'pod info is not known yet'
            tmp_log.debug(err_str)
            return None, err_str

        if _namespace and _scheduler_pod_name and _session_pod_name and _pilot_pod_name and workspec.status != WorkSpec.ST_running:
            # wait for pilot pod to start
            try:
                status, _, stderr = dask_utils.wait_until_deployment(name=_pilot_pod_name, state='Running|Error', namespace=_namespace)
            except Exception as exc:
                err_str = f'caught exception: {exc}'
                tmp_log.warning(err_str)
                status = WorkSpec.ST_failed
                workspec.set_status(status)
                return status, err_str
            else:
                tmp_log.debug(f'pilot pod is running (status={status}, interactive mode={_mode})')

            # wait for the worker pods to start
            try:
                status, pods = dask_utils.await_worker_deployment(_namespace,
                                                                      scheduler_pod_name=_scheduler_pod_name,
                                                                      jupyter_pod_name=_session_pod_name)
            except Exception as exc:
                err_str = f'caught exception: {exc}'
                tmp_log.warning(err_str)
                pods = None
                status = WorkSpec.ST_failed
            else:
                # set workspec.maxWalltime when dask worker pods are running
                # sweeper should kill everything when maxWalltime has been passed
                if status:
                    _status = True
                    for worker in pods:  # dask-workers and pilot
                        pod_info = pods[worker]
                        state = pod_info['status']
                        if 'dask-worker' in worker and state != 'Running':
                            tmp_log.debug(f'worker {worker} is in state={state}')
                            _status = False
                        elif 'pilot' in worker and state == 'Error':
                            tmp_log.debug('pilot failed')
                            _status = False

                    if not _status:
                        tmp_log.debug('setting WorkSpec.ST_failed due to previous error(s)')
                        status = WorkSpec.ST_failed
                    else:
                        workspec.maxWalltime = 900
                        workspec.podStartTime = datetime.datetime.utcnow()
                        tmp_log.debug(f'set podStartTime to {workspec.podStartTime} and maxWalltime to {workspec.maxWalltime}')
                        status = WorkSpec.ST_running
                else:
                    tmp_log.debug(f'worker(s) failed')
                    status = WorkSpec.ST_failed
        else:
            tmp_log.debug(f'will not wait for workers deployment since status={workspec.status}')
        pod_info = self.get_pod_info('pilot', _namespace)
        if pod_info:  # did the pilot finish? if so, get the exit code to see if it finished correctly
            _ec, _status = self.get_pilot_exit_code(pod_info, state='terminated')
            if _status:  # ie an exit code int was correctly received
                if _ec:
                    tmp_log.debug(f'pilot failed with exit code: {_ec}')
                    # clean up
                    dask_utils.remove_local_dir(os.path.join(self._tmpdir, str(_taskid)))
                    # remove everything
                    # ..
                    status = WorkSpec.ST_failed
                else:
                    tmp_log.debug('pilot pod has finished correctly')
                    # if non-interactive mode, terminate everything
                    # ..
            else:
                tmp_log.debug('pilot exit code was not extracted')

        if time_now - workspec.podStartTime > datetime.timedelta(seconds=self.podQueueTimeLimit):
            err_str = f'worker is out of time: {time_now - workspec.podStartTime} s have passed since start (t={time_now})'
            tmp_log.debug(err_str)
            # clean up
            dask_utils.remove_local_dir(os.path.join(self._tmpdir, str(_taskid)))
            # remove everything
            # ..
            status = WorkSpec.ST_finished  # set finished so the job is not retried (??)

        tmp_log.debug(f'setting workspec status={status}')
        workspec.set_status(status)
        return status, err_str

        try:
            pods_list = []  #self.k8s_client.filter_pods_info(self._all_pods_list, job_name=job_id)
            containers_state_list = []
            pods_sup_diag_list = []
            for pod_info in pods_list:
                # make list of status of the pods belonging to our job
                pods_status_list.append(pod_info['status'])
                containers_state_list.extend(pod_info['containers_state'])
                pods_sup_diag_list.append(pod_info['name'])

                # make a list of pods that should be removed
                # 1. pods being queued too long
                if pod_info['status'] in ['Pending', 'Unknown'] and pod_info['start_time'] \
                        and time_now - pod_info['start_time'] > datetime.timedelta(seconds=self.podQueueTimeLimit):
                    pods_name_to_delete_list.append(pod_info['name'])
                # 2. pods with containers in bad states
                if pod_info['status'] in ['Pending', 'Unknown']:
                    for item in pod_info['containers_state']:
                        if item.waiting and item.waiting.reason in BAD_CONTAINER_STATES:
                            pods_name_to_delete_list.append(pod_info['name'])

        except Exception as _e:
            err_str = 'Failed to get POD status of JOB id={0} ; {1}'.format(job_id, _e)
            tmp_log.error(err_str)
            new_status = None
        else:
            if not pods_status_list:
                # there were no pods found belonging to our job
                err_str = 'JOB id={0} not found'.format(job_id)
                tmp_log.error(err_str)
                tmp_log.info('Force to cancel the worker due to JOB not found')
                new_status = WorkSpec.ST_cancelled
            else:
                # we found pods belonging to our job. Obtain the final status
                tmp_log.debug('pods_status_list={0}'.format(pods_status_list))
                new_status, sub_msg = self.check_pods_status(pods_status_list, containers_state_list)
                if sub_msg:
                    err_str += sub_msg
                tmp_log.debug('new_status={0}'.format(new_status))

            # delete pods that have been queueing too long
            if pods_name_to_delete_list:
                tmp_log.debug('Deleting pods queuing too long')
                ret_list = []  #self.k8s_client.delete_pods(pods_name_to_delete_list)
                deleted_pods_list = []
                for item in ret_list:
                    if item['errMsg'] == '':
                        deleted_pods_list.append(item['name'])
                tmp_log.debug('Deleted pods queuing too long: {0}'.format(
                                ','.join(deleted_pods_list)))
            # supplemental diag messages
            sup_error_code = WorkerErrors.error_codes.get('GENERAL_ERROR') if err_str else WorkerErrors.error_codes.get('SUCCEEDED')
            sup_error_diag = 'PODs=' + ','.join(pods_sup_diag_list) + ' ; ' + err_str
            workspec.set_supplemental_error(error_code=sup_error_code, error_diag=sup_error_diag)

        return new_status, err_str

    def check_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, 'queueName={0}'.format(self.queueName), method_name='check_workers')
        tmp_log.debug('start')

        ret_list = list()
        if not workspec_list:
            err_str = 'empty workspec_list'
            tmp_log.debug(err_str)
            ret_list.append(('', err_str))
            return False, ret_list

        pods_info = None  #self.k8s_client.get_pods_info(workspec_list=workspec_list)
        #if pods_info is None:  # there was a communication issue to the K8S cluster
        #    return False, ret_list

        self._all_pods_list = pods_info

        # resolve status requested workers
        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_iterator = thread_pool.map(self.check_a_worker, workspec_list)

        ret_list = list(ret_iterator)
        for workspec in workspec_list:
            tmp_log.debug(f'status={workspec.status}')
        tmp_log.debug('done')
        return True, ret_list
