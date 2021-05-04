"""
utilities routines associated with Kubernetes python client

"""
import os
import copy
import base64
import yaml
import datetime

from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend as crypto_default_backend

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
from pandaharvester.harvestercore import core_utils

base_logger = core_utils.setup_logger('k8s_utils')

CONFIG_DIR = '/scratch/jobconfig'
SHARED_DIR = '/scratch/shared'
DIST_DIR_BASE = '/scratch/dist'
# the pilot has to run on the shared directory, otherwise the evaluation container has no access
WORK_DIR = '{0}/pilot/'.format(SHARED_DIR)

HD_DIR = '/scratch/hostdiscovery'
SSH_DIR = '/scratch/ssh'
HD_FILENAME = 'discover_hosts.sh'
HD_CONT_NAME = 'hd-init-cont'

HOROVOD_WORKER_TAG = 'horovod-workers'
HOROVOD_HEAD_TAG = 'horovod-head'

HOST_DISC_TAG = 'host-discovery'
SSH_KEYS_TAG = 'ssh-keys'

POD_FAILED_STATES = ['CrashLoopBackOff']
POD_QUEUED_STATES = ['Pending', 'Unknown']
POD_RUNNING_STATES = ['Running']
POD_FINISHED_STATES = ['Succeeded']

evaluation_script_fn = 'evaluation_script.sh'
evaluation_script = """
# Copy ssh keys in order to be able to connect to workers 
mkdir -p ~/.ssh; 
cp $SSH_DIR/private_key ~/.ssh/id_rsa;

iteration=0
while :
do
    iteration=$(( $iteration + 1 ))
    echo \"===================== ITERATION $iteration =====================\";
    
    while [ ! -f $SHARED_DIR/payload_workdir/__payload_in_sync_file__ ]; do sleep 5; done; 

    echo \"=== cd to pilot directory ===\";
    cd $SHARED_DIR/payload_workdir/
    
    echo \"=== recreate shared directory in distributed filesystem ===\";
    rm -rf $DIST_DIR;
    mkdir -p $DIST_DIR;

    echo \"=== moving input.json to distributed directory ===\";
    cp workDir/* $DIST_DIR;

    echo \"=== environment information ===\";
    echo \"whoami\";
    whoami;
    echo \"pwd\";
    pwd;
    echo \"ls -lrt\";
    ls -lrt;
    echo \"ls workDir\";
    ls workDir;
    echo;

    echo \"=== cat exec script ===\"; 
    cat $SHARED_DIR/payload_workdir/__run_main_exec.sh; 
    echo; 

    echo \"=== exec script ===\"; 
    /bin/sh $SHARED_DIR/payload_workdir/__run_main_exec.sh 2>&1 | tee $SHARED_DIR/payload_workdir/run_main_exec.log;
    REAL_MAIN_RET_CODE=$?;
    
    echo \"=== finished with ===\";
    echo $REAL_MAIN_RET_CODE;
    echo; 

    echo \"=== moving output.json to the pilot directory ===\";
    cp $DIST_DIR/output.json $SHARED_DIR/payload_workdir/workDir
     
    # Copy anything in the shared directory to the pilot directory
    cp -R $DIST_DIR $SHARED_DIR/payload_workdir/output_$iteration;
    rm -rf $DIST_DIR;
     
    # Create the out sync file and delete the in sync file
    echo \"=== create out sync file and delete in sync file ===\";
    touch $SHARED_DIR/payload_workdir/__payload_out_sync_file__;
    rm -f $SHARED_DIR/payload_workdir/__payload_in_sync_file__; 
    
    # exit $REAL_MAIN_RET_CODE;
done
"""

pilot_script_fn = 'pilot.sh'
pilot_script = """
mkdir -p $WORK_DIR;
cp $CONFIG_DIR/* $WORK_DIR;
python3 /user/share/panda-pilot/pilot.py -a {0} -q {1} --pilot-user=ATLAS --harvester-submit-mode=PUSH -t
"""


class k8s_Client(object):

    def __init__(self, namespace, config_file=None):
        if not os.path.isfile(config_file):
            raise RuntimeError('Cannot find k8s config file: {0}'.format(config_file))
        config.load_kube_config(config_file=config_file)
        self.namespace = namespace if namespace else 'default'
        self.core_v1 = client.CoreV1Api()
        self.batch_v1 = client.BatchV1Api()
        self.apps_v1 = client.AppsV1Api()
        self.delete_v1 = client.V1DeleteOptions(propagation_policy='Background')

    def pod_queued_too_long(self, pod, queue_limit):
        time_now = datetime.datetime.utcnow()
        if pod['status'] in POD_QUEUED_STATES and pod['start_time'] \
                and time_now - pod['start_time'] > datetime.timedelta(seconds=queue_limit):
            return True
        return False

    def read_yaml_file(self, yaml_file):
        with open(yaml_file) as f:
            yaml_content = yaml.load(f, Loader=yaml.FullLoader)

        return yaml_content

    def create_job_from_yaml(self, yaml_content, work_spec, prod_source_label, container_image,  executable, args,
                             cert, cert_in_secret=True, cpu_adjust_ratio=100, memory_adjust_ratio=100, max_time=None):

        tmp_log = core_utils.make_logger(base_logger, method_name='create_job_from_yaml')

        # consider PULL mode as default, unless specified
        submit_mode = 'PULL'

        # create the configmap in push mode
        worker_id = None
        if work_spec.mapType != 'NoJob':
            submit_mode = 'PUSH'
            worker_id = str(work_spec.workerID)
            res = self.create_configmap(work_spec)
            if not res:  # if the configmap creation failed, don't submit a job because the pod creation will hang
                return res, 'Failed to create a configmap'

        # retrieve panda queue information
        panda_queues_dict = PandaQueuesDict()
        queue_name = panda_queues_dict.get_panda_queue_name(work_spec.computingSite)

        # set the worker name
        yaml_content['metadata']['name'] = yaml_content['metadata']['name'] + "-" + str(work_spec.workerID)

        # set the resource type and other metadata to filter the pods
        yaml_content['spec']['template'].setdefault('metadata', {})
        yaml_content['spec']['template']['metadata'].update({'labels':
                                                                 {'resourceType': str(work_spec.resourceType),
                                                                  'prodSourceLabel': str(prod_source_label),
                                                                  'pq': str(work_spec.computingSite)
                                                                 }
                                                             })

        # fill the container details. we can only handle one container (take the first, delete the rest)
        yaml_containers = yaml_content['spec']['template']['spec']['containers']
        del (yaml_containers[1:len(yaml_containers)])

        container_env = yaml_containers[0]

        container_env.setdefault('resources', {})
        # set the container image
        if 'image' not in container_env:
            container_env['image'] = container_image

        if 'command' not in container_env:
            container_env['command'] = executable
            container_env['args'] = args

        # set the resources (CPU and memory) we need for the container
        # note that predefined values in the yaml template will NOT be overwritten
        # Be familiar with QoS classes: https://kubernetes.io/docs/tasks/configure-pod-container/quality-service-pod
        # The CPU & memory settings will affect the QoS for the pod
        container_env.setdefault('resources', {})
        if work_spec.nCore > 0:

            # CPU limits
            container_env['resources'].setdefault('limits', {})
            if 'cpu' not in container_env['resources']['limits']:
                container_env['resources']['limits']['cpu'] = str(work_spec.nCore)
            # CPU requests
            container_env['resources'].setdefault('requests', {})
            if 'cpu' not in container_env['resources']['requests']:
                container_env['resources']['requests']['cpu'] = str(work_spec.nCore * cpu_adjust_ratio / 100.0)

        if work_spec.minRamCount > 4:  # K8S minimum memory limit = 4 MB
            # memory limits
            # container_env['resources'].setdefault('limits', {})
            # if 'memory' not in container_env['resources']['limits']:
            #     container_env['resources']['limits']['memory'] = str(work_spec.minRamCount) + 'M'
            # memory requests
            container_env['resources'].setdefault('requests', {})
            if 'memory' not in container_env['resources']['requests']:
                container_env['resources']['requests']['memory'] = str(
                    work_spec.minRamCount * memory_adjust_ratio / 100.0) + 'M'

        container_env.setdefault('env', [])
        # try to retrieve the stdout log file name
        try:
            log_file_name = work_spec.workAttributes['stdout']
        except (KeyError, AttributeError):
            tmp_log.debug('work_spec does not have stdout workAttribute, using default')
            log_file_name = ''

        container_env['env'].extend([
            {'name': 'computingSite', 'value': work_spec.computingSite},
            {'name': 'pandaQueueName', 'value': queue_name},
            {'name': 'resourceType', 'value': work_spec.resourceType},
            {'name': 'prodSourceLabel', 'value': prod_source_label},
            {'name': 'jobType', 'value': work_spec.jobType},
            {'name': 'proxySecretPath', 'value': cert if cert_in_secret else None},
            {'name': 'proxyContent', 'value': None if cert_in_secret else self.set_proxy(cert)},
            {'name': 'workerID', 'value': str(work_spec.workerID)},
            {'name': 'logs_frontend_w', 'value': harvester_config.pandacon.pandaCacheURL_W},
            {'name': 'logs_frontend_r', 'value': harvester_config.pandacon.pandaCacheURL_R},
            {'name': 'stdout_name', 'value': log_file_name},
            {'name': 'PANDA_JSID', 'value': 'harvester-' + harvester_config.master.harvester_id},
            {'name': 'HARVESTER_WORKER_ID', 'value': str(work_spec.workerID)},
            {'name': 'HARVESTER_ID', 'value': harvester_config.master.harvester_id},
            {'name': 'submit_mode', 'value': submit_mode}
        ])

        # in push mode, add the configmap as a volume to the pod
        if submit_mode == 'PUSH' and worker_id:
            yaml_content['spec']['template']['spec'].setdefault('volumes', [])
            yaml_volumes = yaml_content['spec']['template']['spec']['volumes']
            yaml_volumes.append({'name': 'job-config', 'configMap': {'name': worker_id}})
            # mount the volume to the filesystem
            container_env.setdefault('volumeMounts', [])
            container_env['volumeMounts'].append({'name': 'job-config', 'mountPath': CONFIG_DIR})

        # if we are running the pilot in a emptyDir with "pilot-dir" name, then set the max size
        if 'volumes' in yaml_content['spec']['template']['spec']:
            yaml_volumes = yaml_content['spec']['template']['spec']['volumes']
            for volume in yaml_volumes:
                # do not overwrite any hardcoded sizeLimit value
                if volume['name'] == 'pilot-dir' and 'emptyDir' in volume and 'sizeLimit' not in volume['emptyDir']:
                    maxwdir_prorated_GB = panda_queues_dict.get_prorated_maxwdir_GB(work_spec.computingSite,
                                                                                    work_spec.nCore)
                    if maxwdir_prorated_GB:
                        volume['emptyDir']['sizeLimit'] = '{0}G'.format(maxwdir_prorated_GB)

        # set the affinity
        if 'affinity' not in yaml_content['spec']['template']['spec']:
            yaml_content = self.set_affinity(yaml_content)

        # set max_time to avoid having a pod running forever
        if 'activeDeadlineSeconds' not in yaml_content['spec']['template']['spec']:
            if not max_time:  # 4 days
                max_time = 4 * 24 * 23600
            yaml_content['spec']['template']['spec']['activeDeadlineSeconds'] = max_time

        tmp_log.debug('creating job {0}'.format(yaml_content))

        rsp = self.batch_v1.create_namespaced_job(body=yaml_content, namespace=self.namespace)
        return rsp, yaml_content

    def generate_ls_from_wsl(self, workspec_list=[]):
        if workspec_list:
            batch_ids_list = [workspec.batchID for workspec in workspec_list if workspec.batchID]
            batch_ids_concat = ','.join(batch_ids_list)
            label_selector = '{0} in ({1})'.format('job-name', batch_ids_concat)
        else:
            label_selector = ''

        return label_selector

    def get_pods_info(self, workspec_list=[]):

        tmp_log = core_utils.make_logger(base_logger, method_name='get_pods_info')
        pods_list = list()

        label_selector = self.generate_ls_from_wsl(workspec_list)
        # tmp_log.debug('label_selector: {0}'.format(label_selector))

        try:
            ret = self.core_v1.list_namespaced_pod(namespace=self.namespace, label_selector=label_selector)
        except Exception as _e:
            tmp_log.error('Failed call to list_namespaced_pod with: {0}'.format(_e))
        else:
            for i in ret.items:
                pod_info = {
                    'name': i.metadata.name,
                    'start_time': i.status.start_time.replace(tzinfo=None) if i.status.start_time else i.status.start_time,
                    'status': i.status.phase,
                    'status_conditions': i.status.conditions,
                    'job_name': i.metadata.labels['job-name'] if i.metadata.labels and 'job-name' in i.metadata.labels else None,
                    'containers_state': []
                }
                if i.status.container_statuses:
                    for cs in i.status.container_statuses:
                        if cs.state:
                            pod_info['containers_state'].append(cs.state)
                pods_list.append(pod_info)

        return pods_list

    def filter_pods_info(self, pods_list, job_name=None):
        if job_name:
            pods_list = [i for i in pods_list if i['job_name'] == job_name]
        return pods_list

    def get_jobs_info(self, workspec_list=[]):

        tmp_log = core_utils.make_logger(base_logger, method_name='get_jobs_info')

        jobs_list = list()

        label_selector = self.generate_ls_from_wsl(workspec_list)
        # tmp_log.debug('label_selector: {0}'.format(label_selector))

        try:
            ret = self.batch_v1.list_namespaced_job(namespace=self.namespace, label_selector=label_selector)

            for i in ret.items:
                job_info = {
                    'name': i.metadata.name,
                    'status': i.status.conditions[0].type,
                    'status_reason': i.status.conditions[0].reason,
                    'status_message': i.status.conditions[0].message
                }
                jobs_list.append(job_info)
        except Exception as _e:
            tmp_log.error('Failed call to list_namespaced_job with: {0}'.format(_e))

        return jobs_list

    def delete_pods(self, pod_name_list):
        ret_list = list()

        for pod_name in pod_name_list:
            rsp = {'name': pod_name}
            try:
                self.core_v1.delete_namespaced_pod(name=pod_name, namespace=self.namespace, body=self.delete_v1,
                                                  grace_period_seconds=0)
            except ApiException as _e:
                rsp['errMsg'] = '' if _e.status == 404 else _e.reason
            except Exception as _e:
                rsp['errMsg'] = _e.reason
            else:
                rsp['errMsg'] = ''
            ret_list.append(rsp)

        return ret_list

    def delete_job(self, job_name):
        tmp_log = core_utils.make_logger(base_logger, 'job_name={0}'.format(job_name), method_name='delete_job')
        try:
            self.batch_v1.delete_namespaced_job(name=job_name, namespace=self.namespace, body=self.delete_v1,
                                               grace_period_seconds=0)
        except Exception as _e:
            tmp_log.error('Failed call to delete_namespaced_job with: {0}'.format(_e))

    def delete_config_map(self, config_map_name):
        self.core_v1.delete_namespaced_config_map(name=config_map_name, namespace=self.namespace, body=self.delete_v1,
                                                 grace_period_seconds=0)

    def delete_secret(self, secret_name):
        self.core_v1.delete_namespaced_secret(name=secret_name, namespace=self.namespace, body=self.delete_v1,
                                              grace_period_seconds=0)

    def set_proxy(self, proxy_path):
        with open(proxy_path) as f:
            content = f.read()
        content = content.replace("\n", ",")
        return content

    def set_affinity(self, yaml_content):
        yaml_content['spec']['template']['spec']['affinity'] = {}
        yaml_affinity = yaml_content['spec']['template']['spec']['affinity']
        res_element = {'SCORE', 'MCORE'}
        affinity_spec = {
            'preferredDuringSchedulingIgnoredDuringExecution': [
                {'weight': 100, 'podAffinityTerm': {
                    'labelSelector': {'matchExpressions': [
                        {'key': 'resourceType', 'operator': 'In', 'values': ['SCORE']}]},
                    'topologyKey': 'kubernetes.io/hostname'}
                 }]}

        resource_type = yaml_content['spec']['template']['metadata']['labels']['resourceType']

        if resource_type == 'SCORE':
            yaml_affinity['podAffinity'] = copy.deepcopy(affinity_spec)
            yaml_affinity['podAffinity']['preferredDuringSchedulingIgnoredDuringExecution'][0]['podAffinityTerm'][
                'labelSelector']['matchExpressions'][0]['values'][0] = resource_type

        yaml_affinity['podAntiAffinity'] = copy.deepcopy(affinity_spec)
        yaml_affinity['podAntiAffinity']['preferredDuringSchedulingIgnoredDuringExecution'][0]['podAffinityTerm'][
            'labelSelector']['matchExpressions'][0]['values'][0] = res_element.difference({resource_type}).pop()

        return yaml_content

    def create_or_patch_secret(self, file_list, secret_name):
        # api_version = 'v1'
        # kind = 'Secret'
        # type='kubernetes.io/tls'
        rsp = None
        tmp_log = core_utils.make_logger(base_logger, method_name='create_or_patch_secret')

        metadata = {'name': secret_name, 'namespace': self.namespace}
        data = {}
        for file_name in file_list:
            filename = os.path.basename(file_name)
            with open(file_name, 'rb') as f:
                content = f.read()
            data[filename] = base64.b64encode(content).decode()
        body = client.V1Secret(data=data, metadata=metadata)
        try:
            try:
                rsp = self.core_v1.patch_namespaced_secret(name=secret_name, body=body, namespace=self.namespace)
                tmp_log.debug('Patched secret')
            except ApiException as e:
                tmp_log.debug('Exception when patching secret: {0} . Try to create secret instead...'.format(e))
                rsp = self.core_v1.create_namespaced_secret(body=body, namespace=self.namespace)
                tmp_log.debug('Created secret')
        except Exception as e:
            tmp_log.error('Exception when patching or creating secret: {0}.'.format(e))
        return rsp

    def create_configmap(self, work_spec):
        # useful guide:
        # https://matthewpalmer.net/kubernetes-app-developer/articles/ultimate-configmap-guide-kubernetes.html

        worker_id = str(work_spec.workerID)
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
                                         method_name='create_configmap')
        try:
            # Get the access point. The messenger should have dropped the input files for the pilot here
            access_point = work_spec.get_access_point()
            pjd = 'pandaJobData.out'
            job_data_file = os.path.join(access_point, pjd)
            with open(job_data_file) as f:
                job_data_contents = f.read()

            pfc = 'PoolFileCatalog_H.xml'
            pool_file_catalog_file = os.path.join(access_point, pfc)
            with open(pool_file_catalog_file) as f:
                pool_file_catalog_contents = f.read()

            # put the job data and PFC into a dictionary
            data = {pjd: job_data_contents, pfc: pool_file_catalog_contents}

            # instantiate the configmap object
            metadata = {'name': worker_id, 'namespace': self.namespace}
            config_map = client.V1ConfigMap(api_version="v1", kind="ConfigMap", data=data, metadata=metadata)

            # create the configmap object in K8s
            api_response = self.core_v1.create_namespaced_config_map(namespace=self.namespace, body=config_map)
            tmp_log.debug('Created configmap')
            return True

        except (ApiException, TypeError) as e:
            tmp_log.error('Could not create configmap with: {0}'.format(e))
            return False

    def create_configmap_horovod(self, work_spec, panda_queue):
        # useful guide:
        # https://matthewpalmer.net/kubernetes-app-developer/articles/ultimate-configmap-guide-kubernetes.html

        worker_id = str(work_spec.workerID)
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
                                         method_name='create_configmap_horovod')
        try:
            # Get the access point. The messenger should have dropped the input files for the pilot here
            access_point = work_spec.get_access_point()
            pjd = 'pandaJobData.out'
            job_data_file = os.path.join(access_point, pjd)
            with open(job_data_file) as f:
                job_data_contents = f.read()

            pfc = 'PoolFileCatalog_H.xml'
            pool_file_catalog_file = os.path.join(access_point, pfc)
            with open(pool_file_catalog_file) as f:
                pool_file_catalog_contents = f.read()

            # put the job data and PFC into a dictionary
            data = {pjd: job_data_contents,
                    pfc: pool_file_catalog_contents,
                    evaluation_script_fn: evaluation_script,
                    pilot_script_fn: pilot_script.format(WORK_DIR, panda_queue)
                    }

            # instantiate the configmap object
            metadata = {'name': worker_id, 'namespace': self.namespace}
            config_map = client.V1ConfigMap(api_version="v1", kind="ConfigMap", data=data, metadata=metadata)

            # create the configmap object in K8s
            api_response = self.core_v1.create_namespaced_config_map(namespace=self.namespace, body=config_map)
            tmp_log.debug('Created configmap')
            return True

        except (ApiException, TypeError) as e:
            tmp_log.error('Could not create configmap with: {0}'.format(e))
            return False

    def create_ssh_keys_secret(self, work_spec):
        # useful guide:
        # https://matthewpalmer.net/kubernetes-app-developer/articles/ultimate-configmap-guide-kubernetes.html

        worker_id = str(work_spec.workerID)
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
                                         method_name='create_ssh_keys_secret')
        try:

            key = rsa.generate_private_key(
                backend=crypto_default_backend(),
                public_exponent=65537,
                key_size=2048
            )

            private_key = key.private_bytes(crypto_serialization.Encoding.PEM,
                                            crypto_serialization.PrivateFormat.PKCS8,
                                            crypto_serialization.NoEncryption())

            public_key = key.public_key().public_bytes(crypto_serialization.Encoding.OpenSSH,
                                                       crypto_serialization.PublicFormat.OpenSSH)

            name = '{0}-{1}'.format(SSH_KEYS_TAG, worker_id)

            metadata = {'name': name, 'namespace': self.namespace}
            data = {'private_key': base64.b64encode(private_key).decode(),
                    'public_key': base64.b64encode(public_key).decode()}
            body = client.V1Secret(data=data, metadata=metadata)

            rsp = self.core_v1.create_namespaced_secret(body=body, namespace=self.namespace)
            tmp_log.debug('Created secret with ssh keys')
            return True

        except (ApiException, TypeError) as e:
            tmp_log.error('Could not create secret with ssh keys. Error: {0}'.format(e))
            return False

    def create_host_discovery_configmap(self, work_spec):
        # useful guide:
        # https://matthewpalmer.net/kubernetes-app-developer/articles/ultimate-configmap-guide-kubernetes.html
        # Horovod formations require additionally to create a host discovery script
        # This script will initially be empty and filled by the formation monitor once the pod IPs are known

        worker_id = str(work_spec.workerID)
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
                                         method_name='create_host_discovery_configmap')
        try:
            hosts = ''
            data = {HD_FILENAME: hosts}

            # instantiate the configmap object
            metadata = {'name': '{0}-{1}'.format(HOST_DISC_TAG, worker_id), 'namespace': self.namespace}
            config_map = client.V1ConfigMap(api_version="v1", kind="ConfigMap", data=data, metadata=metadata)

            # create the configmap object in K8s
            api_response = self.core_v1.create_namespaced_config_map(namespace=self.namespace, body=config_map)
            tmp_log.debug('Created configmap')
            return True

        except (ApiException, TypeError) as e:
            tmp_log.error('Could not create configmap with: {0}'.format(e))
            return False

    def update_host_discovery_configmap(self, work_spec, host_list):

        worker_id = str(work_spec.workerID)
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
                                         method_name='update_host_discovery_configmap')

        try:
            if not host_list:
                tmp_log.debug('host list is empty'.format(host_list))

            hosts = ''
            for host in host_list:
                hosts += 'echo {0}:1\n'.format(host)
            data = {HD_FILENAME: hosts}

            # instantiate the configmap object
            name = '{0}-{1}'.format(HOST_DISC_TAG, worker_id)

            metadata = {'name': name, 'namespace': self.namespace}
            config_map = client.V1ConfigMap(api_version="v1", kind="ConfigMap", data=data, metadata=metadata)
            # update the configmap object in K8s
            api_response = self.core_v1.patch_namespaced_config_map(name, self.namespace, config_map)

            tmp_log.debug('Created configmap')
            return True

        except (ApiException, TypeError) as e:
            tmp_log.error('Could not create configmap with: {0}'.format(e))
            return False

    def get_pod_logs(self, pod_name, previous=False):
        tmp_log = core_utils.make_logger(base_logger, method_name='get_pod_logs')
        try:
            rsp = self.core_v1.read_namespaced_pod_log(name=pod_name, namespace=self.namespace, previous=previous)
            tmp_log.debug('Log file retrieved for {0}'.format(pod_name))
        except Exception as e:
            tmp_log.debug('Exception when getting logs for pod {0} : {1}. Skipped'.format(pod_name, e))
            raise
        else:
            return rsp

    def fill_hpo_head_container_template(self, work_spec, image, command, cert=None, name=None):

        worker_id = work_spec.workerID
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
                                         method_name='fill_hpo_head_container_template')

        volume_mounts = []

        # TODO: the request & limit values need to be extracted from the job
        resources = client.V1ResourceRequirements(requests={"cpu": "1500m", "memory": "3000Mi"},
                                                  limits={"cpu": "1500m", "memory": "3000Mi"})

        # The distributed shared directory for horovod head and workers communication
        dist_dir = os.path.join(DIST_DIR_BASE, str(worker_id))

        env = [client.V1EnvVar(name='computingSite', value=work_spec.computingSite),
               client.V1EnvVar(name='SHARED_DIR', value=SHARED_DIR),
               client.V1EnvVar(name='CONFIG_DIR', value=CONFIG_DIR),
               client.V1EnvVar(name='DIST_DIR', value=dist_dir),
               client.V1EnvVar(name='HD_DIR', value=HD_DIR),
               client.V1EnvVar(name='SSH_DIR', value=SSH_DIR)
               ]

        if name == 'pilot' and cert:
            try:
                # Attach secret with proxy
                proxy_dir = os.path.split(cert)[0]
                proxy_mount = client.V1VolumeMount(name='proxy-secret', mount_path=proxy_dir)
                volume_mounts.append(proxy_mount)

                # Add a env variable for the pilot to know the proxy file
                env.append(client.V1EnvVar(name='X509_USER_PROXY', value=cert))
                tmp_log.debug(cert)

                # Add env variable to indicate to the pilot to skip the payload
                env.append(client.V1EnvVar(name='HARVESTER_HOROVOD', value='True'))

                # Rucio client needs to know the account for stage-in/out
                env.append(client.V1EnvVar(name='RUCIO_ACCOUNT', value='pilot'))

                # Add env variable to indicate the work directory of the pilot
                env.append(client.V1EnvVar(name='WORK_DIR', value=WORK_DIR))

                # Add env variable to indicate post process to wait for out sync file
                env.append(client.V1EnvVar(name='PAYLOAD_TANDEM_MODE', value='True'))

            except:
                pass

        # Attach config-map containing job details
        configmap_mount = client.V1VolumeMount(name='job-config', mount_path=CONFIG_DIR)
        volume_mounts.append(configmap_mount)

        # Attach config-map containing host discovery script
        hd_configmap_mount = client.V1VolumeMount(name='host-discovery', mount_path=HD_DIR)
        volume_mounts.append(hd_configmap_mount)

        # Attach secret with ssh keys
        ssh_keys_mount = client.V1VolumeMount(name='ssh-keys', mount_path=SSH_DIR)
        volume_mounts.append(ssh_keys_mount)

        # Attach shared volume between pilot and evaluation containers
        shared_mount = client.V1VolumeMount(name='shared-dir', mount_path=SHARED_DIR)
        volume_mounts.append(shared_mount)

        # Attach distributed (NFS) volume between head and workers
        dist_mount = client.V1VolumeMount(name='dist-dir', mount_path=DIST_DIR_BASE)
        volume_mounts.append(dist_mount)

        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Container.md
        container = client.V1Container(name=name, image=image, resources=resources, env=env,
                                       volume_mounts=volume_mounts,
                                       command=command, image_pull_policy='Always')

        return container

    def fill_hd_init_container_template(self):
        # this init container waits until the host discovery file has been filled out,
        # so should be as lightweight as possible

        # set the container_template image and command
        image = 'busybox'
        command = ['sh', '-c', 'while true; do sleep 1; if [ -s $HD_FILE ]; then exit 0; fi; done;']

        resources = client.V1ResourceRequirements(requests={"cpu": "1000m", "memory": "2000Mi"},
                                                  limits={"cpu": "2000m", "memory": "3000Mi"})

        env = [client.V1EnvVar(name='HD_FILE', value='{0}/{1}'.format(HD_DIR, HD_FILENAME))]

        # Attach config-map containing host discovery script
        hd_configmap_mount = client.V1VolumeMount(name='host-discovery', mount_path=HD_DIR)

        # https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Container.md
        container = client.V1Container(name=HD_CONT_NAME, image=image, resources=resources, env=env,
                                       volume_mounts=[hd_configmap_mount],
                                       command=command)

        return container

    def create_horovod_head(self, work_spec, panda_queue, evaluation_image,
                            pilot_image, cert, dfs_claim_name,
                            cpu_adjust_ratio=100, memory_adjust_ratio=100, max_time=None):

        worker_id = str(work_spec.workerID)
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
                                         method_name='create_horovod_head')

        if not self.create_configmap_horovod(work_spec, panda_queue):
            return False

        if not self.create_host_discovery_configmap(work_spec):
            return False

        # generate init container that waits for workers to be available
        # Elastic horovod fails if it starts and no worker is available

        # generate pilot and evaluation container
        pilot_command = ["sh", "-c", "sh $CONFIG_DIR/{0}".format(pilot_script_fn)]
        pilot_container = self.fill_hpo_head_container_template(work_spec, pilot_image, pilot_command,
                                                                cert=cert, name='pilot')
        evaluation_command = ["sh", "-c", "sh $CONFIG_DIR/{0}".format(evaluation_script_fn)]
        evaluation_container = self.fill_hpo_head_container_template(work_spec, evaluation_image, evaluation_command,
                                                                     name='evaluation')

        # generate init containers
        init_hd_container = self.fill_hd_init_container_template()

        # generate secret, configmap and shared directory volumes
        # documentation: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1Volume.md
        proxy_secret = client.V1Volume(name='proxy-secret',
                                       secret=client.V1SecretVolumeSource(secret_name='proxy-secret'))

        ssh_secret_name = 'ssh-keys-{0}'.format(worker_id)
        ssh_keys = client.V1Volume(name='ssh-keys',
                                   secret=client.V1SecretVolumeSource(secret_name=ssh_secret_name,
                                                                      default_mode=0o600))

        config_map = client.V1Volume(name='job-config',
                                     config_map=client.V1ConfigMapVolumeSource(name=worker_id))

        hd_config_map = client.V1Volume(name='host-discovery',
                                        config_map=client.V1ConfigMapVolumeSource(name='{0}-{1}'.format(HOST_DISC_TAG,
                                                                                                        worker_id),
                                                                                  default_mode=0o755))

        shared_dir = client.V1Volume(name='shared-dir',
                                     empty_dir=client.V1EmptyDirVolumeSource())

        dist_dir = client.V1Volume(name='dist-dir',
                                   persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=dfs_claim_name))

        node_selector = {'processor': 'cpu'}
        # create the pod spec with the containers and volumes
        # documentation: https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1PodSpec.md
        max_time = 4 * 24 * 23600
        spec = client.V1PodSpec(containers=[pilot_container, evaluation_container],
                                volumes=[proxy_secret, ssh_keys, config_map, hd_config_map, shared_dir, dist_dir],
                                init_containers=[init_hd_container],
                                active_deadline_seconds=max_time,
                                node_selector=node_selector,
                                restart_policy='Never')

        pod = client.V1Pod(spec=spec, metadata=client.V1ObjectMeta(name='{0}-{1}'.format(HOROVOD_HEAD_TAG, worker_id),
                                                                   labels={'app': '{0}-{1}'.format(HOROVOD_HEAD_TAG,
                                                                                                   worker_id)}))

        tmp_log.debug('creating pod {0}'.format(pod))

        rsp = self.core_v1.create_namespaced_pod(body=pod, namespace=self.namespace)
        return rsp

    def create_horovod_workers(self, work_spec, container_image,  command, dfs_claim_name,
                               cpu_adjust_ratio=100, memory_adjust_ratio=100, max_time=None):

        worker_id = str(work_spec.workerID)
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id),
                                         method_name='create_horovod_workers')

        # The distributed shared directory for horovod head and workers communication
        dist_dir = os.path.join(DIST_DIR_BASE, str(worker_id))

        env = [client.V1EnvVar(name='SSH_DIR', value=SSH_DIR),
               client.V1EnvVar(name='DIST_DIR', value=dist_dir)]

        ssh_keys_mount = client.V1VolumeMount(name='ssh-keys', mount_path=SSH_DIR)

        # Attach distributed (NFS) volume between head and workers
        dist_mount = client.V1VolumeMount(name='dist-dir', mount_path=DIST_DIR_BASE)

        deployment_name = "{0}-{1}".format(HOROVOD_WORKER_TAG, worker_id)

        # TODO: decide how the memory and CPU/GPU requirements will be calculated
        resources = client.V1ResourceRequirements(requests={"cpu": "7000m", "memory": "14Gi"},
                                                  limits={"cpu": "7300m", "memory": "14.1Gi"})

        container = client.V1Container(command=command, name=HOROVOD_WORKER_TAG, image=container_image,
                                       volume_mounts=[ssh_keys_mount, dist_mount], env=env, resources=resources,
                                       image_pull_policy='Always')

        node_selector = {'processor': 'gpu'}

        ssh_secret_name = 'ssh-keys-{0}'.format(worker_id)
        ssh_secret_volume = client.V1Volume(name='ssh-keys',
                                            secret=client.V1SecretVolumeSource(secret_name=ssh_secret_name,
                                                                               default_mode=0o600))
        dist_dir = client.V1Volume(name='dist-dir',
                                   persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=dfs_claim_name))

        pod_spec = client.V1PodSpec(containers=[container],
                                    volumes=[ssh_secret_volume, dist_dir],
                                    node_selector=node_selector)

        template = client.V1PodTemplateSpec(metadata=client.V1ObjectMeta(labels={"app": "{0}-{1}".format(HOROVOD_WORKER_TAG,
                                                                                                         worker_id)}),
                                            spec=pod_spec)

        # TODO: decide how many workers are required
        spec = client.V1DeploymentSpec(replicas=2, template=template,
                                       selector={"matchLabels": {"app": "{0}-{1}".format(HOROVOD_WORKER_TAG,
                                                                                         worker_id)}})

        deployment = client.V1Deployment(api_version="apps/v1", kind="Deployment",
                                         metadata=client.V1ObjectMeta(name=deployment_name,
                                                                      labels={"app": "{0}-{1}".format(HOROVOD_WORKER_TAG,
                                                                                                      worker_id)}),
                                         spec=spec)

        tmp_log.debug('creating deployment {0}'.format(deployment))

        rsp = self.apps_v1.create_namespaced_deployment(body=deployment, namespace=self.namespace)
        return rsp

    def create_horovod_formation(self, work_spec, prod_source_label, panda_queue,
                                 evaluation_image, pilot_image,
                                 worker_command, cert, dfs_claim_name,
                                 cpu_adjust_ratio=100, memory_adjust_ratio=100,
                                 max_time=None):

        rsp = self.create_ssh_keys_secret(work_spec)
        if not rsp:
            return rsp

        rsp = self.create_horovod_head(work_spec, panda_queue, evaluation_image, pilot_image, cert, dfs_claim_name,
                                       cpu_adjust_ratio, memory_adjust_ratio, max_time)
        if not rsp:
            return rsp

        rsp = self.create_horovod_workers(work_spec, evaluation_image, worker_command, dfs_claim_name,
                                          cpu_adjust_ratio, memory_adjust_ratio, max_time)

        if not rsp:
            return rsp

        return True

    def generate_horovod_ls(self, workspec_list, tag):

        if not workspec_list or not tag:
            return ''

        ids_list = ['{0}-{1}'.format(tag, workspec.workerID) for workspec in workspec_list if workspec.workerID]
        ids_concat = ','.join(ids_list)
        label_selector = '{0} in ({1})'.format('app', ids_concat)

        return label_selector

    def translate_container_statuses(self, container_statuses):

        cs = {}

        if not container_statuses:
            return cs

        for container in container_statuses:
            status = None
            if container.state.running:
                status = 'running'
            elif container.state.terminated:
                if container.state.terminated.reason == 'Completed':
                    status = 'finished'
                else:
                    status = 'failed'
            elif container.state.waiting:
                if container.state.waiting.reason in POD_FAILED_STATES:
                    status = 'failed'
                else:
                    status = 'pending'
            cs[container.name] = status

        return cs

    def resolve_head_states(self, container_statuses, init_container_statuses):

        main_container_states = self.translate_container_statuses(container_statuses)
        init_container_states = self.translate_container_statuses(init_container_statuses)

        # merge both dictionaries
        container_states = {**main_container_states , **init_container_states}
        return container_states

    def get_horovod_formations_info(self, workspec_list=[]):

        tmp_log = core_utils.make_logger(base_logger, method_name='get_horovod_formations_info')
        formations_info = {}

        # get the status of the head pod
        head_ls = self.generate_horovod_ls(workspec_list, HOROVOD_HEAD_TAG)
        try:
            ret = self.core_v1.list_namespaced_pod(namespace=self.namespace, label_selector=head_ls)
        except Exception as _e:
            tmp_log.error('Failed call to list_namespaced_pod with: {0}'.format(_e))
        else:
            for i in ret.items:
                worker_id = int(i.metadata.name[len(HOROVOD_HEAD_TAG)+1:])

                head_container_states = self.resolve_head_states(i.status.container_statuses, i.status.init_container_statuses)
                tmp_log.debug('head container statuses: {0}'.format(i.status.container_statuses))
                tmp_log.debug('head init container statuses: {0}'.format(i.status.init_container_statuses))
                tmp_log.debug('head container states: {0}'.format(head_container_states))

                pod_info = {'name': i.metadata.name,
                            'start_time': i.status.start_time.replace(tzinfo=None) if i.status.start_time else i.status.start_time,
                            'status': i.status.phase,
                            'status_conditions': i.status.conditions,
                            'app': i.metadata.labels['app'] if i.metadata.labels and 'app' in i.metadata.labels else None,
                            'container_states': head_container_states
                            }

                formations_info.setdefault(worker_id, {})
                formations_info[worker_id]['head_pod'] = pod_info

        # get the status of the worker deployment
        worker_ls = self.generate_horovod_ls(workspec_list, HOROVOD_WORKER_TAG)
        try:
            ret = self.apps_v1.list_namespaced_deployment(namespace=self.namespace, label_selector=worker_ls)
        except Exception as _e:
            tmp_log.error('Failed call to list_namespaced_deployment with: {0}'.format(_e))
        else:
            for i in ret.items:
                # TODO: think about how to properly monitor the deployments
                worker_id = int(i.metadata.name[len(HOROVOD_WORKER_TAG) + 1:])
                dep_info = {'name': i.metadata.name,
                            'available_replicas': i.status.available_replicas,
                            'unavailable_replicas': i.status.unavailable_replicas,
                            'status_conditions': i.status.conditions,
                            'app': i.metadata.labels['app'] if i.metadata.labels and 'app' in i.metadata.labels else None,
                            'containers_state': {}
                            }
                formations_info.setdefault(worker_id, {})
                formations_info[worker_id]['worker_deployment'] = dep_info

        # get details of the individual workers
        try:
            ret = self.core_v1.list_namespaced_pod(namespace='default', label_selector=worker_ls)
        except Exception as _e:
            tmp_log.error('Failed call to list_namespaced_pod with: {0}'.format(_e))
        else:
            for i in ret.items:
                worker_id = int(i.metadata.name[len(HOROVOD_WORKER_TAG) + 1:].split('-')[0])
                if i.status.phase in POD_RUNNING_STATES and i.status.pod_ip:
                    formations_info[worker_id].setdefault('worker_pods', [])

                    pod_info = {'status': i.status.phase,
                                'ip': i.status.pod_ip}

                    formations_info[worker_id]['worker_pods'].append(pod_info)

        tmp_log.debug("Found following Horovod formations: {0}".format(formations_info))

        return formations_info

    def delete_horovod_formation(self, work_spec):

        worker_id = str(work_spec.workerID)
        tmp_log = core_utils.make_logger(base_logger, 'workerID={0}'.format(worker_id), method_name='delete_horovod_formation')
        err_str = ''

        # TODO: these calls can be encapsulated a bit nicer

        # delete the worker deployment. This is done deliberately as first step of the clean up, since it's the
        # most expensive component of the formation
        try:
            dep_name = '{0}-{1}'.format(HOROVOD_WORKER_TAG, worker_id)
            api_response = self.apps_v1.delete_namespaced_deployment(name=dep_name, namespace=self.namespace,
                                                                     body=self.delete_v1, grace_period_seconds=0)
            tmp_log.debug('Deleted worker deployment')
        except ApiException as _e:
            # if the worker dep did not exist, don't consider it an error
            if _e.status != 404 and _e.reason != 'Not Found':
                err_str += 'Failed to delete the WORKER DEP; {1}'.format(_e)

        # delete the heads
        try:
            head_name = '{0}-{1}'.format(HOROVOD_HEAD_TAG, worker_id)
            api_response = self.core_v1.delete_namespaced_pod(name=head_name, namespace=self.namespace,
                                                              body=self.delete_v1, grace_period_seconds=0)
            tmp_log.debug('Deleted head pod for {0}'.format(worker_id))
        except ApiException as _e:
            # if the head pod did not exist, don't consider it an error
            if _e.status != 404 and _e.reason != 'Not Found':
                err_str = 'Failed to delete the HEAD POD; {1}'.format(_e)

        # delete the configmaps with the job configuration and the host discovery
        config_maps = []
        config_maps.append(worker_id)  # job info
        config_maps.append('{0}-{1}'.format(HOST_DISC_TAG, worker_id))  # host discovery file
        for config_map in config_maps:
            try:
                self.delete_config_map(config_map)
                tmp_log.debug('Deleted configmap: {0}'.format(config_map))
            except ApiException as _e:
                # if the configmap did not exist, don't consider it an error
                if _e.status != 404 and _e.reason != 'Not Found':
                    err_str = 'Failed to delete a CONFIGMAP ; {0}'.format(_e)

        # delete the secrets
        secrets = []
        secrets.append('{0}-{1}'.format(SSH_KEYS_TAG, worker_id))  # ssh keys for head-worker communication
        for secret in secrets:
            try:
                self.delete_secret(secret)
                tmp_log.debug('Deleted secret: {0}'.format(secret))
            except ApiException as _e:
                # if the secret did not exist, don't consider it an error
                if _e.status != 404 and _e.reason != 'Not Found':
                    err_str = 'Failed to delete a CONFIGMAP ; {0}'.format( _e)

        if err_str:
            tmp_log.error(err_str)
            return False, err_str

        return True, None
