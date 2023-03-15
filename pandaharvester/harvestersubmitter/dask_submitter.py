# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2022

import os
import re
import json
import argparse
import traceback
from shutil import rmtree
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
from pandaharvester.harvestersubmitter.dask_submitter_base import DaskSubmitterBase
from pandaharvester.harvestercore.work_spec import WorkSpec

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
ERROR_REMOTEDIR = 10

# default user secrets (overwritten by job definition)
USERNAME = 'user'
PASSWORD = 'user'

# submitter for Dask
class DaskSubmitter(PluginBase):

    # default values
    _nworkers = 1
    _namespace = ''
    _userid = ''
    _ispvc = False  # set when PVC is successfully created
    _ispv = False  # set when PV is successfully created
    _password = None
    _interactive_mode = True
    _tmpdir = ''
    _nfs_server = "10.226.152.66"
    _project = "gke-dev-311213"
    _zone = "europe-west1-b"
    _local_workdir = ''
    _remote_workdir = ''  # only set this once the remote workdir has been created
    _mountpath = 'nfs-client:/mnt/dask'

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
        self.proxySecretPath = os.getenv('PROXY_SECRET_PATH', None)

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

    def _choose_proxy(self, workspec, is_grandly_unified_queue):
        """
        Choose the proxy based on the job type and whether k8s secrets are enabled
        """
        cert = None
        job_type = workspec.jobType

        # use same for now..
        if is_grandly_unified_queue and job_type in ('user', 'panda', 'analysis'):
            cert = self.proxySecretPath
        else:
            cert = self.proxySecretPath

        if not cert:
            cert = '/cephfs/atlpan/harvester/proxy/x509up_u25606_prod'

        return cert

    def get_maxtime(self, panda_queue_dict):

        try:
            max_time = panda_queue_dict['maxtime']
        except IndexError:
            tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='get_maxtime')
            tmp_log.warning(f'Could not retrieve maxtime field for queue {self.queueName}')
            max_time = None

        return max_time

    def makedir(self, directory):
        """
        Create a local directory. Ignore tmpdir if it already exists.

        :param directory: full path directory (string).
        :return: exit code (int), diagnostics (string).
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='mkdir')

        exit_code = 0
        diagnostics = ''

        if not os.path.exists(directory):
            try:
                dask_utils.mkdirs(directory)
            except Exception as exc:
                diagnostics = f'failed to create directory {directory}: {exc}'
                tmp_log.error(diagnostics)
                exit_code = ERROR_MKDIR
            else:
                tmp_log.debug(f'created destination directory at {directory}')
        else:
            if directory == self._tmpdir:
                pass
            else:
                exit_code = ERROR_MKDIR
                diagnostics = f'directory {directory} already exists'
                tmp_log.warning(diagnostics)

        return exit_code, diagnostics

    def place_job_def(self, job_spec=None, scheduler_ip=None, session_ip=None):
        """
        Create and place the job definition file in the default user area.
        If self._local_workdir exists after this function has finished, it will be removed as it is no longer needed.

        Scheduler IP is the internal IP number for the dask scheduler.
        Session IP is the external IP number for the session (e.g. jupyterlab).
        These IP numbers are added to the job definition dictionary sent to the pilot pod, which will add them to the
        job metrics message sent to the PanDA server (updateJob instruction).

        :param job_spec: job spec object.
        :param scheduler_ip: dask scheduler internal IP (string).
        :param session_ip: session external IP (string).
        :return: exit code (int), diagnostics (string).
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='place_job_def')

        exit_code = 0
        diagnostics = ''

        tmp_log.debug(f'processing job {job_spec.PandaID} (create local and remote work directories)')
        job_spec_dict = dask_utils.to_dict(job_spec)
        if scheduler_ip:
            job_spec_dict['scheduler_ip'] = scheduler_ip
        if session_ip:
            job_spec_dict['session_ip'] = session_ip
        tmp_log.debug(f'job_spec_dict={job_spec_dict}')

        # pilot pod will create user space; submitter will create job definition and push it to /mnt/dask
        # where it will be discovered by the pilot pod (who will know the job id and therefore which job def to pull)
        # for now, only push the job def to /mnt/dask on the shared file system

        # place the job def in the local workdir and move it recursively to the remote shared file system
        filepath = os.path.join(self._local_workdir, f'pandaJobData.out')
        try:
            # create job def in local dir - to be moved to remove location
            with open(filepath, 'w') as _file:
                json.dump(job_spec_dict, _file)
            tmp_log.debug(f'wrote file {filepath}')
            tmp_log.debug(f'attempting to copy {self._local_workdir} to remote FileStore')
            # only file copy: cmd = f'gcloud compute scp {filepath} {self._mountpath} --project {self._project} --zone {self._zone}'
            # copy local dir: e.g. gcloud compute scp --recurse /tmp/panda/12345678 nfs-client:/mnt/dask --project "gke-dev-311213" --zone "europe-west1-b"
            cmd = f'gcloud compute scp --recurse {self._local_workdir} {self._mountpath} --project {self._project} --zone {self._zone}'
            exitcode, stdout, stderr = dask_utils.execute(cmd)
            if stderr:
                exit_code = ERROR_WRITEFILE if exitcode == 0 else exitcode
                diagnostics = f'failed:\n{stderr}'
                tmp_log.error(diagnostics)
            else:
                tmp_log.debug(stdout)
        except Exception as exc:
            diagnostics = f'failed to create file {filepath}: {exc}'
            tmp_log.error(diagnostics)
            exit_code = ERROR_WRITEFILE
            return exit_code, diagnostics

        return exit_code, diagnostics

    def store_remote_directory_path(self, pandaid):
        """
        Extract and store the path to the remote directory.

        :param pandaid: PanDA id (int).
        :return: exit code (int), diagnostics (string).
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='store_remote_directory')
        exit_code = 0
        diagnostics = ''

        pattern = r'[A-Za-z\-]+\:(.+)'
        found = re.findall(pattern, self._mountpath)
        if found:
            # set the full path to the remote directory - this will later be sent to the remote-cleanup pod
            self._remote_workdir = os.path.join(found[0], f'{pandaid}')
            tmp_log.debug(f'remote workdir={self._remote_workdir}')
        else:
            diagnostics = f'failed to set _remote_workdir from pattern={pattern}, _mountpath={self._mountpath}, PandaID={pandaid}'
            tmp_log.error(diagnostics)
            exit_code = ERROR_REMOTEDIR

        return exit_code, diagnostics

    def read_yaml_file(self, yaml_file):
        """
        Read the contents of the given yaml file

        :param yaml_file: yaml file name (string).
        :return: yaml content (string).
        """

        with open(yaml_file) as f:
            yaml_content = yaml.load(f, Loader=yaml.FullLoader)

        return yaml_content

    def remove_local_dir(self, directory):
        """
        Remove the given local directory.

        :param directory: directory name (string).
        :return:
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='remove_local_dir')

        try:
            rmtree(directory)
        except OSError as exc:
            tmp_log.warning(exc)
        else:
            tmp_log.info(f'removed local directory {directory}')

    def create_workdir(self, pandaid):
        """
        Create the local workdir.

        :return: exit code (int), diagnostics (string).
        """

        exit_code = 0
        diagnostics = ""

        # the local directory can be removed once the job spec has been moved to the remote location
        self._tmpdir = os.environ.get('DASK_TMPDIR', '/tmp/panda')
        self._local_workdir = os.path.join(self._tmpdir, f'{pandaid}')
        dirs = [self._tmpdir, self._local_workdir]
        for directory in dirs:
            exit_code, diagnostics = self.makedir(directory)

        return exit_code, diagnostics

    def get_number_of_namespaces(self):
        """

        """

        nspaces = 0
        _, stdout, _ = dask_utils.execute("kubectl get namespaces")
        if stdout:
            nspaces = len(dask_utils._convert_to_dict(stdout))

        return nspaces

    def submit_harvester_worker(self, work_spec):
        """
        Submit the harvester worker.

        Note: there is one base dask submitter per panda job.

        :param work_spec: work spec object.
        :return:
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='submit_harvester_worker')
        timing = {'t0': time.time()}
        tmp_return_value = (False, 'error diagnostics not set')

        nspace = self.get_number_of_namespaces()
        tmp_log.debug(f'there are {nspace} namespaces')
        if nspace > 4:
            tmp_log.debug('should probably return for now')

        if work_spec.status == WorkSpec.ST_running or work_spec.status == WorkSpec.ST_failed:
            err_str = 'this job has already been processed'
            tmp_log.debug(err_str)
            return (False, err_str)
        else:
            tmp_log.debug(f'workspec.status={work_spec.status}')

        # get info from harvester queue config
        _queueConfigMapper = QueueConfigMapper()
        harvester_queue_config = _queueConfigMapper.get_queue(self.queueName)

        # set the stdout log file
        log_file_name = f'{harvester_config.master.harvester_id}_{work_spec.workerID}.out'
        work_spec.set_log_file('stdout', f'{self.logBaseURL}/{log_file_name}')

        # place the job definition in the shared user area
        job_spec_list = work_spec.get_jobspec_list()
        # note there really should only be a single job
        if len(job_spec_list) > 1:
            tmp_log.warning(f'can only handle single dask job: found {len(job_spec_list)} jobs!')
        job_spec = job_spec_list[0]

        # create the job work dir locally
        exit_code, diagnostics = self.create_workdir(job_spec.PandaID)
        if exit_code != 0:
            return exit_code, diagnostics

        # k8s_yaml_file=/data/atlpan/k8_configs/job_prp_driver_ssd.yaml
        # yaml_content = self.read_yaml_file(self.k8s_yaml_file)
        #if not yaml_content:
        #    # handle error
        #    err_str = f'failed to read yaml file {self.k8s_yaml_file}'
        #    tmp_log.warning(err_str)
        #    return (False, err_str)

        submitter = None
        try:
            # read the job configuration (if available, only push model)
            job_fields, job_pars_parsed = self.read_job_configuration(work_spec)

            # decide container image. In pull mode, defaults are provided
            # container_image: "atlasadc/atlas-grid-centos7"
            container_image = self.decide_container_image(job_fields, job_pars_parsed)

            # choose the appropriate proxy
            this_panda_queue_dict = self.panda_queues_dict.get(self.queueName, dict())
            is_grandly_unified_queue = self.panda_queues_dict.is_grandly_unified_queue(self.queueName)
            cert = self._choose_proxy(work_spec, is_grandly_unified_queue)
            if not cert:
                err_str = 'No proxy specified in proxySecretPath. Not submitted'
                tmp_log.warning(err_str)
                return (False, err_str)

            # get the walltime limit
            # max_time = self.get_maxtime(this_panda_queue_dict)

            # input parameters [to be passed to the install function]
            harvester_workdir = os.environ.get('HARVESTER_WORKDIR', '/data/atlpan/harvester/workdir')
            nworkers = 2  # number of dask workers
            interactive_mode = True  # True means interactive jupyterlab session, False means pilot pod runs user payload
            session_type = 'jupyterlab'  # Later try with 'ContainerSSH'
            userid = ''.join(random.choice(ascii_lowercase) for _ in range(5))  # unique 5-char user id (basically for K8)
            namespace = f'single-user-{userid}'
            work_spec.namespace = namespace
            tmp_log.debug(f'using namespace={work_spec.namespace}')
            # try statement in case secrets are not provided in job_spec
            try:
                username, password = self.get_secrets(job_spec)
            except Exception as exc:
                tmp_log.warning(f'exception caught: {exc}')
                username = USERNAME
                password = PASSWORD
                # return (False, 'no user secrets found')

            # get the user image, if set
            user_image = self.get_user_image(job_spec)
            tmp_log.debug(f'user image={user_image}')

            # store the remote directory path for later removal (return error code in case of failure)
            # exit_code, diagnostics = store_remote_directory_path(job_spec.PandaID)
            self._remote_workdir = os.path.join(self._mountpath, str(job_spec.PandaID))

            # instantiate the base dask submitter here
            tmp_log.debug(f'initializing DaskSubmitterBase for user {userid} in namespace {namespace}')
            submitter = DaskSubmitterBase(nworkers=nworkers,
                                          username=username,
                                          password=password,
                                          interactive_mode=interactive_mode,
                                          session_type=session_type,
                                          local_workdir=harvester_workdir,
                                          remote_workdir=self._remote_workdir,
                                          userid=userid,
                                          namespace=namespace,
                                          pandaid=job_spec.PandaID,
                                          workspec=work_spec,
                                          queuename=self.queueName)
            if submitter:
                info = 'not set yet'
                exitcode, service_info, diagnostics = submitter.install(timing)
                if exitcode:
                    err_str = f'failed with exit code={exitcode}, diagnostics={diagnostics}'
                    tmp_log.warning(err_str)

                    tmp_log.warning(f'deleting remote workdir: {self._remote_workdir}')
                    submitter.deploy_cleanup(self._remote_workdir)

                    work_spec.set_status(WorkSpec.ST_failed)
                    job_spec.status = 'failed'
                    tmp_return_value = (False, err_str)
                elif service_info:
                    # IP numbers should now be known
                    info = '\ndask scheduler has external ip %s' % service_info['dask-scheduler'].get('external_ip')
                    info += '\ndask scheduler has internal ip %s' % service_info['dask-scheduler'].get('internal_ip')
                    info += '\njupyterlab has external ip %s' % service_info['jupyterlab'].get('external_ip')
                    tmp_log.info(info)

                    # communicate IP numbers to pilot pod via the job definition
                    # i.e. now it's time to place the job definition in the shared area, /mnt/dask/[job id]
                    # create the job definition both locally and remotely
                    exit_code, diagnostics = self.place_job_def(job_spec=job_spec,
                                                                scheduler_ip=service_info['dask-scheduler'].get('internal_ip'),
                                                                session_ip=service_info['jupyterlab'].get('external_ip'))
                    # always remove the local work dir immediately     - WHY?
                    if self._local_workdir and os.path.exists(self._local_workdir):
                        self.remove_local_dir(self._local_workdir)
                    if exit_code:
                        # handle error
                        err_str = f'place_job_def() failed with exit code {exit_code} (aborting)'
                        tmp_log.warning(err_str)
                        tmp_return_value = (False, err_str)

                # done, cleanup and exit
                if interactive_mode:
                    # create the clean-up script [eventually to be executed by sweeper instead of executing cleanup() below?]
                    submitter.create_cleanup_script(work_spec.workerID)
                else:
                    # pilot pod should be done - clean-up everything (skip if sweeper deletes everything?)
                    submitter.cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
                submitter.timing_report(timing, info=info)
            else:
                err_str = 'DaskSubmitterBase did not complete install()'
                tmp_log.warning(err_str)
                tmp_return_value = (False, err_str)
        except Exception as exc:
            if submitter:
                submitter.cleanup(namespace=submitter.get_namespace(), user_id=submitter.get_userid(), pvc=True, pv=True)
            tmp_log.error(traceback.format_exc())
            err_str = f'Failed to create a JOB; {exc}'
            tmp_return_value = (False, err_str)
        else:
            work_spec.batchID = f'kubernetes-job-{userid}'  # yaml_content['metadata']['name']
            tmp_log.debug(f'Created harvester worker {work_spec.workerID} with batchID={work_spec.batchID}')
            tmp_return_value = (True, '')

        return tmp_return_value

    def get_secrets(self, job_spec):
        """
        Extract the secret user information.

        :param job_spec: job spec dictionary.
        :return: username (string), password (string).
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='get_secrets')

        username = USERNAME
        password = PASSWORD
        _secret = None

        job_spec_dict = dask_utils.to_dict(job_spec)
        job = job_spec_dict.get(job_spec.PandaID)
        secrets_str = job.get('secrets', None)  # json string
        if secrets_str:
            secrets = json.loads(secrets_str)
            for key in secrets.keys():
                _secret = secrets[key]
                break
            if _secret:
                _secret = json.loads(_secret)
                username = _secret.get('username', 'user')
                password = _secret.get('password', 'trustno1')
        else:
            tmp_log.warning(f'no secrets in job definition - using default values (panda id={job_spec.PandaID})')

        return username, password

    def get_user_image(self, job_spec):
        """
        Extract the user image.

        :param job_spec: job spec dictionary.
        :return: user image name (string).
        """

        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='get_user_image')

        job_spec_dict = dask_utils.to_dict(job_spec)
        job = job_spec_dict.get(job_spec.PandaID)
        return job.get('container_name', None)  # json string

    # submit workers (and scheduler)
    def submit_workers(self, workspec_list):
        tmp_log = self.make_logger(base_logger, f'queueName={self.queueName}', method_name='submit_workers')

        n_workers = len(workspec_list)
        base_logger.debug(f'start, n_workers={n_workers}')

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
