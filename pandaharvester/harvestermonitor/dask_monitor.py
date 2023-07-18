import datetime
import os.path
import time
from json import loads
from typing import Any

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
    _harvester_workdir = os.environ.get('HARVESTER_WORKDIR', '/data/atlpan/harvester/workdir')
    _pilot_archive = os.environ.get('HARVESTER_PILOT_ARCHIVE', '/data/atlpan/harvester/archive')

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
            self.podQueueTimeLimit = 24 * 60 * 60

        self._all_pods_list = []

    def get_pod_info(self, podname: str, namespace: str) -> dict:
        """
        Get the pod_info dictionary containing all the known pod information.

        :param podname: pod name (string)
        :param namespace: name space (string)
        :return: pod info dictionary.
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='get_pod_info')

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
            except IndexError as exc:
                tmp_log.warning(f'caught exception: {exc}')
        else:
            if 'not found' in stderr:
                tmp_log.debug(f'pod {podname} not found: {stderr}')
            else:
                tmp_log.warning(f'command (cmd) failed: {stderr}')

        return pod_info

    def get_pilot_exit_code(self, pod_info: dict, state: str = 'unknown') -> (int, bool):
        """
        Get the pilot container exit code.
        Note: a status Boolean is also returned to make sure that the exit code was correctly extracted.

        :param pod_info: pod info (dict)
        :param state: state (str)
        :return: exit code (int), state (bool).
        """

        # pod_info['containers_state'][0]['terminated']['exitCode']

        tmp_log = self.make_logger(base_logger, method_name='get_pilot_exit_code')

        status = False
        exit_code = 0
        if state not in pod_info['containers_state'][0]:
            tmp_log.warning(f'state {state} not found in pod_info={pod_info}')
        else:
            try:
                exit_code = pod_info['containers_state'][0][state]['exitCode']
            except IndexError as exc:
                tmp_log.warning(f'caught exception: {exc}')
            else:
                status = True if isinstance(exit_code, int) else False

        return exit_code, status

    def check_pods_status(self, pods_status_list: list, containers_state_list: list) -> (int, str):  # noqa: C901
        """
        Check status of pods.

        :param pods_status_list: status list for pods (list)
        :param containers_state_list: state list for containers (list)
        :return: status (int), message (string).
        """

        # tmp_log = self.make_logger(base_logger, method_name='check_pods_status')
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
                            msg_str = f'container not terminated yet ({state}) while pod Succeeded'
                        elif item.terminated.reason != 'Completed':
                            msg_str = f'container terminated by k8s for reason {item.terminated.reason}'
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

    def check_a_worker(self, workspec: Any) -> (int, str):  # noqa: C901
        """
        Check a worker.

        :param workspec: workspec object
        :return: status (int), error string.
        """

        # set logger
        tmp_log = self.make_logger(base_logger, f'workerID={workspec.workerID} batchID={workspec.batchID}',
                                   method_name='check_a_worker')

        # initialization
        status = None
        # job_id = workspec.batchID
        err_str = ''
        pods_sup_diag_list = []
        time_now = datetime.datetime.utcnow()
        pilot_start = None  # will be set when the pilot pod is running
        # pods_status_list = []
        # pods_name_to_delete_list = []

        # extract the namespace, scheduler and session pod names from the encoded workspec.namespace
        if workspec.namespace:
            _namespace, _taskid, _mode, _leasetime, _scheduler_pod_name, _session_pod_name, _pilot_pod_name = dask_utils.extract_pod_info(workspec.namespace)
        else:
            err_str = 'pod info is not known yet'
            tmp_log.debug(err_str)
            return None, err_str

        if _namespace and _scheduler_pod_name and _session_pod_name and _pilot_pod_name and workspec.status != WorkSpec.ST_running:
            # wait for pilot pod to start
            try:
                status, _, _ = dask_utils.wait_until_deployment(name=_pilot_pod_name, state='Running|Error', namespace=_namespace)
            except Exception as exc:
                err_str = f'caught exception: {exc}'
                tmp_log.warning(err_str)
                status = WorkSpec.ST_failed
                workspec.set_status(status)
                return status, err_str
            else:
                tmp_log.debug(f'pilot pod is running (status={status}, interactive mode={_mode})')

                # start checking lease time once the pilot pod is in running state (cannot know the payload state here)
                if status and _mode == 'interactive':
                    pilot_start = int(time.time())
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
                tmp_log.debug(f'checking status for pods={pods}')
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
                    tmp_log.debug('worker(s) failed')
                    status = WorkSpec.ST_failed
        else:
            tmp_log.debug(f'will not wait for workers deployment since status={workspec.status}')

        pod_info = self.get_pod_info('pilot', _namespace)
        if pod_info:  # did the pilot finish? if so, get the exit code to see if it finished correctly
            pods_sup_diag_list.append('pilot')
            _ec, _status = self.get_pilot_exit_code(pod_info, state='terminated')
            if _status:  # ie an exit code int was correctly received

                # backup pilot log
                path = os.path.join(self._pilot_archive, f'{_taskid}-pilotlog.txt')
                cmd = f'kubectl logs pilot -n {_namespace} >{path}'
                dask_utils.execute(cmd)

                if _ec:
                    err_str = f'pilot failed with exit code: {_ec}'
                    tmp_log.debug(err_str)

                    # clean up
                    dask_utils.remove_local_dir(os.path.join(self._tmpdir, str(_taskid)))

                    # remove everything
                    self.delete_job(workspec.workerID, _taskid)
                    status = WorkSpec.ST_failed
                else:
                    tmp_log.debug('pilot pod has finished correctly')
                    # clean up
                    dask_utils.remove_local_dir(os.path.join(self._tmpdir, str(_taskid)))
                    # remove everything
                    self.delete_job(workspec.workerID, _taskid)
                    status = WorkSpec.ST_finished  # set finished so the job is not retried (??)
            else:
                tmp_log.debug('pilot exit code was not extracted')

        # is the lease time up?
        if pilot_start and (int(time.time()) - pilot_start > _leasetime):
            err_str = f'payload is out of time: {int(time.time()) - pilot_start} s have passed since pilot pod started (lease time={_leasetime} s)'
            tmp_log.debug(err_str)
            # clean up
            dask_utils.remove_local_dir(os.path.join(self._tmpdir, str(_taskid)))
            # remove everything
            self.delete_job(workspec.workerID, _taskid)
            status = WorkSpec.ST_finished  # set finished so the job is not retried (??)

        # are we out of time?
        if workspec.podStartTime and (time_now - workspec.podStartTime > datetime.timedelta(seconds=self.podQueueTimeLimit)):
            err_str = f'worker is out of time: {time_now - workspec.podStartTime} s have passed since start (t={time_now})'
            tmp_log.debug(err_str)
            # clean up
            dask_utils.remove_local_dir(os.path.join(self._tmpdir, str(_taskid)))
            # remove everything
            self.delete_job(workspec.workerID, _taskid)
            status = WorkSpec.ST_finished  # set finished so the job is not retried (??)

        # supplemental diag messages
        sup_error_code = WorkerErrors.error_codes.get('GENERAL_ERROR') if err_str else WorkerErrors.error_codes.get('SUCCEEDED')
        sup_error_diag = 'PODs=' + ','.join(pods_sup_diag_list) + ' ; ' + err_str if err_str else ''
        workspec.set_supplemental_error(error_code=sup_error_code, error_diag=sup_error_diag)
        tmp_log.debug(f'setting workspec status={status}, sup_error_code={sup_error_code}, sup_error_diag={sup_error_diag}')
        workspec.set_status(status)
        return status, err_str

    def delete_job(self, worker_id: int, task_id: int):
        """
        Clean up.

        :param worker_id: worker id (int)
        :param task_id: task id (int).
        """

        tmp_log = self.make_logger(base_logger, f'workerID={worker_id}', method_name='sweep_worker')

        # cleanup namespace
        path = os.path.join(self._harvester_workdir, f'{worker_id}-cleanup.sh')
        if os.path.exists(path):
            tmp_log.debug(f'cleaning up after worker_id={worker_id}')
            ec, stdout, stderr = dask_utils.execute(path)
            if ec:
                tmp_log.debug(f'failed to execute {path}: {stdout}\n{stderr}')
            else:
                tmp_log.debug(f'executed {path}')
        else:
            tmp_log.warning(f'path={path} does not exist (failed to cleanup)')

        # clean the harvester workdir for the current job
        for _id in (task_id, worker_id):
            cmd = f'rm -f {self._harvester_workdir}/{_id}-*'
            tmp_log.debug(f'cleaning up harvester workdir ({cmd})')
            ec, stdout, stderr = dask_utils.execute(cmd)
            if ec:
                tmp_log.warning(f'failed with ec={ec}, out={stdout+stderr}')

    def check_workers(self, workspec_list: list) -> (bool, list):
        """
        Check workers.

        :param workspec_list: list of workspecs (list)
        :return: status (bool), list of workspecs (list).
        """
        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='check_workers')

        ret_list = []
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
