from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestersweeper.base_sweeper import BaseSweeper
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestermisc import dask_utils

import datetime
import os

# logger
base_logger = core_utils.setup_logger('dask_sweeper')


# sweeper for dask
class DaskSweeper(BaseSweeper):
    # constructor

    _harvester_workdir = os.environ.get('HARVESTER_WORKDIR', '/data/atlpan/harvester/workdir')

    def __init__(self, **kwarg):
        BaseSweeper.__init__(self, **kwarg)

        # retrieve the k8s namespace from CRIC
        self.panda_queues_dict = PandaQueuesDict()
        #namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)
        #self.k8s_client = k8s_Client(namespace, queue_name=self.queueName, config_file=self.k8s_config_file)

    # kill workers
    def kill_workers(self, work_spec_list):
        tmp_log = self.make_logger(base_logger, method_name='kill_workers')

        ret_list = []
        tmp_log.debug(f'len(work_spec_list)={len(work_spec_list)}')
        for work_spec in work_spec_list:
            tmp_ret_val = (None, 'Nothing done')

            #time_now = datetime.datetime.utcnow()
            batch_id = work_spec.batchID
            worker_id = str(work_spec.workerID)
            if batch_id:  # sometimes there are missed workers that were not submitted

                # delete the job
                try:
                    #self.k8s_client.delete_job(batch_id)
                    path = os.path.join(self._harvester_workdir, f'{worker_id}-cleanup.sh')
                    if os.path.exists(path):
                        tmp_log.debug(f'cleaning up after worker_id={worker_id}')
                        ec, stdout, stderr = dask_utils.execute(path)
                        if ec:
                            tmp_log.debug(f'failed to execute {path}: {stdout}\n{stderr}')
                        else:
                            tmp_log.debug(f'executed {path}')

                except Exception as _e:
                    err_str = 'Failed to delete a JOB with id={0} ; {1}'.format(batch_id, _e)
                    tmp_log.error(err_str)
                    tmp_ret_val = (False, err_str)

            else:  # the worker does not need be cleaned
                tmp_ret_val = (True, '')

            ret_list.append(tmp_ret_val)

        return ret_list

    def sweep_worker(self, work_spec):
        # cleanup for a worker
        tmp_log = self.make_logger(base_logger, 'workerID={0}'.format(work_spec.workerID), method_name='sweep_worker')

        # retrieve and upload the logs to panda cache
        # batch_id = work_spec.batchID
        # log_content = self.k8s_client.retrieve_pod_log(batch_id)

        # nothing to do
        return True, ''