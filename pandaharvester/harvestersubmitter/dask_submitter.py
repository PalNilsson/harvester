import os
import argparse
#import traceback
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
#from pandaharvester.harvestermisc.k8s_utils import k8s_Client
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
#from pandaharvester.harvestersubmitter import submitter_common

# logger
base_logger = core_utils.setup_logger('dask_submitter')

# image defaults
DEF_SLC6_IMAGE = 'atlasadc/atlas-grid-slc6'
DEF_CENTOS7_IMAGE = 'atlasadc/atlas-grid-centos7'
DEF_IMAGE = DEF_CENTOS7_IMAGE

# command defaults
DEF_COMMAND = ["/usr/bin/bash"]
DEF_ARGS = ["-c", "cd; python $EXEC_DIR/pilots_starter.py || true"]


# submitter for Dask
class DaskSubmitter(PluginBase):

    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        self.panda_queues_dict = PandaQueuesDict()

        # retrieve the k8s namespace from CRIC
        namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)

        # dask_Client() = stand-alone dask submitter
        # self.dask_client = dask_Client(namespace=namespace, queue_name=self.queueName, config_file=self.dask_config_file)

        # required for parsing jobParams
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('-p', dest='executable', type=unquote)
        self.parser.add_argument('--containerImage', dest='container_image')

        # number of processes
        try:
            self.nProcesses
        except AttributeError:
            self.nProcesses = 1
        else:
            if (not self.nProcesses) or (self.nProcesses < 1):
                self.nProcesses = 1

        # x509 proxy through k8s secrets: preferred way
        try:
            self.proxySecretPath
        except AttributeError:
            if os.getenv('PROXY_SECRET_PATH'):
                self.proxySecretPath = os.getenv('PROXY_SECRET_PATH')

        # analysis x509 proxy through k8s secrets: on GU queues
        try:
            self.proxySecretPathAnalysis
        except AttributeError:
            if os.getenv('PROXY_SECRET_PATH_ANAL'):
                self.proxySecretPath = os.getenv('PROXY_SECRET_PATH_ANAL')

    def read_job_configuration(self, work_spec):

        try:
            job_spec_list = work_spec.get_jobspec_list()
            if job_spec_list:
                job_spec = job_spec_list[0]
                job_fields = job_spec.jobParams
                job_pars_parsed = self.parse_params(job_fields['jobPars'])
                return job_fields, job_pars_parsed
        except (KeyError, AttributeError):
            return None, None

        return None, None

    def decide_container_image(self, job_fields, job_pars_parsed):
        """
        Decide container image:
        - job defined image: if we are running in push mode and the job specified an image, use it
        - production images: take SLC6 or CentOS7
        - otherwise take default image specified for the queue
        """
        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='decide_container_image')
        try:
            container_image = job_pars_parsed.container_image
            if container_image:
                tmp_log.debug(f'Taking container image from job params: {container_image}')
                return container_image
        except AttributeError:
            pass

        try:
            cmt_config = job_fields['cmtconfig']
            requested_os = cmt_config.split('@')[1]
            if 'slc6' in requested_os.lower():
                container_image = DEF_SLC6_IMAGE
            else:
                container_image = DEF_CENTOS7_IMAGE
            tmp_log.debug(f'Taking container image from cmtconfig: {container_image}')
            return container_image
        except (KeyError, TypeError):
            pass

        container_image = DEF_IMAGE
        tmp_log.debug(f'Taking default container image: {container_image}')
        return container_image

    def submit_dask_worker(self, work_spec):
        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='submit_dask_worker')

        # get info from harvester queue config
        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

        # set the stdout log file
        log_file_name = f'{harvester_config.master.harvester_id}_{work_spec.workerID}.out'
        work_spec.set_log_file('stdout', f'{self.logBaseURL}/{log_file_name}')

        yaml_content = self.k8s_client.read_yaml_file(self.k8s_yaml_file)
        try:
            # read the job configuration (if available, only push model)
            job_fields, job_pars_parsed = self.read_job_configuration(work_spec)

            # decide container image. In pull mode, defaults are provided
            container_image = self.decide_container_image(job_fields, job_pars_parsed)

            tmp_log.debug(f'container_image: "{container_image}"; args: "{args}"')

            # choose the appropriate proxy
            this_panda_queue_dict = self.panda_queues_dict.get(self.queueName, dict())

            is_grandly_unified_queue = self.panda_queues_dict.is_grandly_unified_queue(self.queueName)
            cert = self._choose_proxy(work_spec, is_grandly_unified_queue)
            if not cert:
                err_str = 'No proxy specified in proxySecretPath. Not submitted'
                tmp_return_value = (False, err_str)
                return tmp_return_value

            # get the walltime limit
            try:
                max_time = this_panda_queue_dict['maxtime']
            except IndexError:
                tmp_log.warning(f'Could not retrieve maxtime field for queue {self.queueName}')
                max_time = None

            prod_source_label = harvester_queue_config.get_source_label(work_spec.jobType)

            # create the scheduler and workers
            # ..

        except Exception as exc:
            tmp_log.error(traceback.format_exc())
            err_str = f'Failed to create a JOB; {exc}'
            tmp_return_value = (False, err_str)
        else:
            work_spec.batchID = yaml_content['metadata']['name']
            tmp_log.debug(f'Created worker {work_spec.workerID} with batchID={work_spec.batchID}')
            tmp_return_value = (True, '')

        return tmp_return_value

    # submit workers (and scheduler)
    def submit_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='submit_workers')

        n_workers = len(workspec_list)
        tmp_log.debug(f'start, n_workers={n_workers}')

        ret_list = list()
        if not workspec_list:
            tmp_log.debug('empty workspec_list')
            return ret_list

        with ThreadPoolExecutor(self.nProcesses) as thread_pool:
            ret_val_list = thread_pool.map(self.submit_dask_worker, workspec_list)
            tmp_log.debug(f'{n_workers} worker(s) submitted')

        ret_list = list(ret_val_list)

        tmp_log.debug('done')

        return ret_list
