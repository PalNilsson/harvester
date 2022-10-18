# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2022

import os
import json
import argparse
import traceback
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor

import random
#import sys
import time
from string import ascii_lowercase
import yaml

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
#from pandaharvester.harvestermisc.k8s_utils import k8s_Client
from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
#from pandaharvester.harvestersubmitter import submitter_common
from pandaharvester.harvestermisc import dask_utils

# logger
base_logger = core_utils.setup_logger('dask_submitter')

# image defaults
DEF_SLC6_IMAGE = 'atlasadc/atlas-grid-slc6'
DEF_CENTOS7_IMAGE = 'atlasadc/atlas-grid-centos7'
DEF_IMAGE = DEF_CENTOS7_IMAGE

# command defaults
DEF_COMMAND = ["/usr/bin/bash"]
DEF_ARGS = ["-c", "cd; python $EXEC_DIR/pilots_starter.py || true"]

# internal error codes
ERROR_NAMESPACE = 1
ERROR_PVPVC = 2
ERROR_CREATESERVICE = 3
ERROR_LOADBALANCER = 4
ERROR_DEPLOYMENT = 5
ERROR_PODFAILURE = 6
ERROR_DASKWORKER = 7
ERROR_MKDIR = 8
ERROR_WRITEFILE = 9

# submitter for Dask
class DaskSubmitter(PluginBase):

    # default values
    _nworkers = 1
    _namespace = ''
    _userid = ''
    _mountpath = 'nfs-client:/mnt/dask'
    _ispvc = False  # set when PVC is successfully created
    _ispv = False  # set when PV is successfully created
    _password = None
    _interactive_mode = True
    _workdir = ''
    _nfs_server = "10.226.152.66"
    _project = "gke-dev-311213"
    _zone = "europe-west1-b"

    # constructor
    def __init__(self, **kwarg):
        self.logBaseURL = None
        PluginBase.__init__(self, **kwarg)

        self.panda_queues_dict = PandaQueuesDict()

        # retrieve the k8s namespace from CRIC
        #namespace = self.panda_queues_dict.get_k8s_namespace(self.queueName)

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

    # from k8s submitter
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

    # from k8s submitter
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

    def get_maxtime(self, panda_queue_dict):

        try:
            max_time = panda_queue_dict['maxtime']
        except IndexError:
            tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='get_maxtime')
            tmp_log.warning(f'Could not retrieve maxtime field for queue {self.queueName}')
            max_time = None

        return max_time

    def place_job_def(self, job_spec):
        """
        Create and place the job definition file in the default user area.
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='place_job_def')

        exit_code = 0
        diagnostics = ''

        tmp_log.debug(f'processing job {job_spec.PandaID}')
        job_spec_dict = dask_utils.to_dict(job_spec)
        tmp_log.debug(f'job_spec_dict={job_spec_dict}')
        destination_dir = os.path.join(self._mountpath, '%s' % job_spec.PandaID)
        tmp_log.debug(f'destination_dir={destination_dir}')

        # pilot pod will create user space; submitter will create job definition and push it to /mnt/dask
        # where it will be discovered by the pilot pod (who will know the job id and therefore which job def to pull)
        # for now, only push the job def to /mnt/dask on the shared file system

#        try:
#            dask_utils.mkdirs(destination_dir)
#        except Exception as exc:
#            diagnostics = f'failed to create directory {destination_dir}: {exc}'
#            tmp_log.error(diagnostics)
#            exit_code = ERROR_MKDIR
#            return exit_code, diagnostics
#        else:
#            tmp_log.debug(f'created destination dir at {destination_dir}')

        filename = f'pandaJobData-{job_spec.PandaID}.out'
        json_object = json.dumps(job_spec_dict)
        try:
            tmp_log.debug(f'attempting to write json to {filename} on remote FileStore')
            #with open(filepath, "w") as outfile:
            #    outfile.write(json_object)
            cmd = f'gcloud compute scp {filename} {self._mountpath} --project {self._project} --zone {self._zone}'
            exitcode, stdout, stderr = dask_utils.execute(cmd)
            if stderr:
                tmp_log.warning(f'failed:\n{stderr}')
            else:
                tmp_log.debug(stdout)
        except Exception as exc:
            diagnostics = f'failed to create file {filepath}: {exc}'
            tmp_log.error(diagnostics)
            exit_code = ERROR_WRITEFILE
            return exit_code, diagnostics
        else:
            tmp_log.debug(f'wrote file {filepath}')

        tmp_log.debug(f'exit_code={exit_code}, diagnostics={diagnostics}')
        return exit_code, diagnostics

    def read_yaml_file(self, yaml_file):
        with open(yaml_file) as f:
            yaml_content = yaml.load(f, Loader=yaml.FullLoader)

        return yaml_content

    def submit_harvester_worker(self, work_spec):
        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='submit_harvester_worker')

        timing = {'t0': time.time()}

        # get info from harvester queue config
        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

        # set the stdout log file
        log_file_name = f'{harvester_config.master.harvester_id}_{work_spec.workerID}.out'
        work_spec.set_log_file('stdout', f'{self.logBaseURL}/{log_file_name}')

        tmp_log.info(f'work_spec={work_spec}')
        # place the job definition in the shared user area
        job_spec_list = work_spec.get_jobspec_list()
        # note there really should only be a single job
        if len(job_spec_list) > 1:
            tmp_log.warning(f'can only handle single dask job: found {len(job_spec_list)} jobs!')
        job_spec = job_spec_list[0]
        tmp_log.info(f'job_spec={job_spec}')

        exit_code, diagnostics = self.place_job_def(job_spec)
        if exit_code:
            # handle error
            tmp_log.debug(f'place_job_def() failed with exit code {exit_code} (aborting)')
            return

        tmp_log.info(f'k8s_yaml_file={self.k8s_yaml_file}')
        yaml_content = self.read_yaml_file(self.k8s_yaml_file)
        try:
            # read the job configuration (if available, only push model)
            job_fields, job_pars_parsed = self.read_job_configuration(work_spec)

            # decide container image. In pull mode, defaults are provided
            container_image = self.decide_container_image(job_fields, job_pars_parsed)
            tmp_log.debug(f'container_image: "{container_image}"')

            # choose the appropriate proxy
            this_panda_queue_dict = self.panda_queues_dict.get(self.queueName, dict())
            is_grandly_unified_queue = self.panda_queues_dict.is_grandly_unified_queue(self.queueName)
            cert = self._choose_proxy(work_spec, is_grandly_unified_queue)
            if not cert:
                err_str = 'No proxy specified in proxySecretPath. Not submitted'
                tmp_return_value = (False, err_str)
                return tmp_return_value

            # get the walltime limit
            max_time = self.get_maxtime(this_panda_queue_dict)

            # not needed: prod_source_label = harvester_queue_config.get_source_label(work_spec.jobType)

            # create the scheduler and workers

            # input parameters [to be passed to the script]
            self._workdir = os.getcwd()  # working directory
            self._nworkers = 2  # number of dask workers
            self._interactive_mode = True  # True means interactive jupyterlab session, False means pilot pod runs user payload
            self._password = 'trustno1'  # jupyterlab password
            self._userid = ''.join(
                random.choice(ascii_lowercase) for _ in range(5))  # unique 5-char user id (basically for K8)
            self._namespace = 'single-user-%s' % self._userid

            # instantiate the base dask submitter here

        except Exception as exc:
            tmp_log.error(traceback.format_exc())
            err_str = f'Failed to create a JOB; {exc}'
            tmp_return_value = (False, err_str)
        else:
            work_spec.batchID = yaml_content['metadata']['name']
            tmp_log.debug(f'Created harvester worker {work_spec.workerID} with batchID={work_spec.batchID}')
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
            # consider putting a timeout to the map function
            ret_val_list = thread_pool.map(self.submit_harvester_worker, workspec_list)
            tmp_log.debug(f'{n_workers} worker(s) submitted')

        tmp_log.debug('done')

        return list(ret_val_list)
