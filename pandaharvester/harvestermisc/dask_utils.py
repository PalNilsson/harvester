# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

#import logging
import os
import re
import subprocess
#import sys
import time
from shutil import rmtree
from json import dump as dumpjson

from pandaharvester.harvestercore import core_utils

base_logger = core_utils.setup_logger('dask_utils')
pilotpoduserid = '1006'
pilotpodgroupid = '1007'

def create_namespace(_namespace, filename):
    """
    Create a namespace for this dask user.

    :param _namespace: namespace (string).
    :param filename: namespace json file name (string).
    :return: True if successful, stderr (Boolean, string).
    """

    namespace_dictionary = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata":
            {
                "name": _namespace, "labels":
                {
                    "name": _namespace
                }
            }
    }

    status = write_json(filename, namespace_dictionary)
    if not status:
        return False

    status, _, stderr = kubectl_apply(filename=filename)

    return status, stderr


def execute(executable, **kwargs):
    """
    Execute the command and its options in the provided executable list.
    The function also determines whether the command should be executed within a container.
    TODO: add time-out functionality.

    :param executable: command to be executed (string or list).
    :param kwargs (timeout, usecontainer, returnproc):
    :return: exit code (int), stdout (string) and stderr (string) (or process if requested via returnproc argument)
    """

    cwd = kwargs.get('cwd', os.getcwd())
    stdout = kwargs.get('stdout', subprocess.PIPE)
    stderr = kwargs.get('stderr', subprocess.PIPE)
    # timeout = kwargs.get('timeout', 120)
    # usecontainer = kwargs.get('usecontainer', False)
    returnproc = kwargs.get('returnproc', False)
    # job = kwargs.get('job')

    # convert executable to string if it is a list
    if isinstance(executable, list):
        executable = ' '.join(executable)

    exe = ['/bin/bash', '-c', executable]
    process = subprocess.Popen(exe,
                               bufsize=-1,
                               stdout=stdout,
                               stderr=stderr,
                               cwd=cwd,
                               preexec_fn=os.setsid,
                               encoding='utf-8',
                               errors='replace'
                               )
    if returnproc:
        return process
    else:
        stdout, stderr = process.communicate()
        exit_code = process.poll()

        return exit_code, stdout, stderr


def kubectl_create(filename=None):
    """
    Execute the kubectl create command for a given yaml file.

    :param filename: yaml or json file name (string).
    :return: True if success (Boolean), stdout (string), stderr (string).
    """

    if not filename:
        return None

    return kubectl_execute(cmd='create', filename=filename)


def kubectl_apply(filename=None):
    """
    Execute the kubectl apply command for a given yaml or json file.

    :param filename: yaml or json file name (string).
    :return: True if success (Boolean), stdout (string), stderr (string).
    """

    if not filename:
        return None

    return kubectl_execute(cmd='apply', filename=filename)


def kubectl_delete(filename=None):
    """
    Execute the kubectl delete command for a given yaml file.

    :param filename: yaml file name (string).
    :return: True if success (Boolean), stdout (string), stderr (string).
    """

    if not filename:
        return None

    return kubectl_execute(cmd='delete', filename=filename)


def kubectl_logs(pod=None, namespace=None):
    """
    Execute the kubectl logs command for a given pod name.

    :param pod: pod name (string).
    :param namespace: namespace (string).
    :return: True if success (Boolean), stdout (string), stderr (string).
    """

    if not pod:
        return None, '', 'pod not set'
    if not namespace:
        return None, '', 'namespace not set'

    return kubectl_execute(cmd='logs', pod=pod, namespace=namespace)


def kubectl_execute(cmd=None, filename=None, pod=None, namespace=None):
    """
    Execute the kubectl create command for a given yaml file or pod name.

    :param cmd: kubectl command (string).
    :param filename: yaml or json file name (string).
    :param pod: pod name (string).
    :param namespace: namespace (string).
    :return: True if success, stdout, stderr (Boolean, string, string).
    """

    base_logger.debug(f'kubectl_execute: cmd={cmd}, filename={filename}, pod={pod}, namespace={namespace}')
    if not cmd:
        stderr = 'kubectl command not set not set'
        base_logger.warning(stderr)
        return None, '', stderr
    if cmd not in ['create', 'delete', 'logs', 'get pods', 'config use-context', 'apply']:
        stderr = 'unknown kubectl command: %s', cmd
        base_logger.warning(stderr)
        return None, '', stderr

    if cmd in ['create', 'delete', 'apply']:
        execmd = 'kubectl %s -f %s' % (cmd, filename)
    elif cmd == 'config use-context':
        execmd = 'kubectl %s %s' % (cmd, namespace)
    else:
        execmd = 'kubectl %s %s' % (cmd, pod) if pod else 'kubectl %s' % cmd

    if cmd in ['get pods', 'logs']:
        execmd += ' --namespace=%s' % namespace

    base_logger.debug('executing: %s', execmd)
    exitcode, stdout, stderr = execute(execmd)
    if exitcode and stderr:
        base_logger.warning('failed:\n%s', stderr)
        status = False
    else:
        base_logger.debug(f'finished executing command: {execmd}')
        status = True

    return status, stdout, stderr


def get_pod_name(namespace=None, pattern=r'(dask\-scheduler\-.+)'):
    """
    Find the name of the pod for the given name pattern in the output from the 'kubectl get pods' command.
    Note: this is intended for findout out the dask-scheduler name. It will not work for dask-workers since there
    will be multiple will similar names (dask-worker-<number>). There is only one scheduler.

    :param namespace: current namespace (string).
    :param pattern: pod name pattern (raw string).
    :return: pod name (string).
    """

    podname = ''

    base_logger.debug(f'get_pod_name called for namespace {namespace}, pattern={pattern}')
    cmd = 'kubectl get pods --namespace %s' % namespace
    exitcode, stdout, stderr = execute(cmd)

    if stderr:
        base_logger.warning('failed:\n%s', stderr)
        return podname
    dictionary = _convert_to_dict(stdout)

    if dictionary:
        for name in dictionary:
            _name = re.findall(pattern, name)
            if _name:
                podname = _name[0]
                break

    return podname


def wait_until_deployment(name=None, state=None, timeout=300, namespace=None, deployment=False, service=False):
    """
    Wait until a given pod or service is in running state.
    In case the service has an external IP, return it.

    Example: name=dask-pilot, state='Running', timeout=120. Function will wait a maximum of 120 s for the
    dask-pilot pod to reach Running state.

    Note: state can also be e.g. Completed|Terminated.

    :param name: pod or service name (string).
    :param state: optional pod status (string).
    :param timeout: time-out (integer).
    :param namespace: namespace (string).
    :param deployment: True for deployments (Boolean).
    :param service: True for services (Boolean).
    :return: True if pod reaches given state before given time-out (Boolean), external IP (string), stderr (string).
    """

    if not name:
        return None, None, 'unset pod/service'

    base_logger.debug(f'name={name},state={state},deployment={deployment},service={service}')
    _external_ip = None
    stderr = ''
    starttime = time.time()
    now = starttime
    _state = None
    _sleep = 5
    first = True
    processing = True
    podtype = 'deployment' if deployment else 'pod'
    podtype = '' if service else podtype  # do not specify podtype for a service
    ip_pattern = r'[0-9]+(?:\.[0-9]+){3}'  # e.g. 1.2.3.4
    port_pattern = r'([0-9]+)\:.'  # e.g. 80:30525/TCP
    dictionary = {}
    while processing and (now - starttime < timeout):

        resource = 'services' if service else name
        cmd = "kubectl get %s %s --namespace=%s" % (podtype, resource, namespace)
        base_logger.debug('executing cmd=\'%s\'', cmd)
        exitcode, stdout, stderr = execute(cmd)
        if stderr and stderr.lower().startswith('error'):
            base_logger.warning('failed:\n%s', stderr)
            break

        dictionary = _convert_to_dict(stdout)
        if dictionary:
            _dic = dictionary.get(name)
            if 'STATUS' in _dic:
                _state = _dic.get('STATUS')
                if _state in state:
                    base_logger.info(f'%s is in state {state}', name)
                    processing = False
                    break
            if 'EXTERNAL-IP' in _dic:  # only look at the load balancer info (dask-scheduler-svc)
                _ip = _dic.get('EXTERNAL-IP')
                ip_number = re.findall(ip_pattern, _ip)
                if ip_number:
                    _external_ip = ip_number[0]
                    status = True  # only for schedulers; if external exists, then we're good
                    # now add the port (e.g. PORT(S)=80:30525/TCP)
            if 'PORT' in _dic:
                _port = _dic.get('PORT')
                port_number = re.findall(port_pattern, _port)
                if port_number and _external_ip:
                    _external_ip += ':%s' % port_number[0]
                    processing = False
                    break
            if first:
                base_logger.debug(f'dictionary=\n{_dic}')
                base_logger.info(f'sleeping until {name} is running (check interval={_sleep}s, timeout={timeout}s)')
                first = False
        time.sleep(_sleep)
        now = time.time()

    if state:
        status = True if (_state and _state in state) else False
    if not status:
        stderr = f'name={name} not running?'
        if dictionary:
            stderr += f' dictionary={dictionary}'
    if dictionary and status:
        base_logger.debug(f'last dictionary={dictionary}')

    if not status:
        cmd = f'kubectl describe pods --namespace={namespace}'
        _, stdout, _ = execute(cmd)
        base_logger.debug(f'{cmd}:\n{stdout}')

    return status, _external_ip, stderr


def _convert_to_dict(stdout):
    """
    Convert table-like stdout to a dictionary.

    :param stdout: command output (string).
    :return: formatted dictionary.
    """

    dictionary = {}

    def remove_parentheses(_line):
        # find all parentheses
        items = re.findall('\(.*?\)', _line)
        if items:
            for item in items:
                _line = _line.replace(item, '')
        return _line.strip()

    first_line = []
    for line in stdout.split('\n'):
        if not line:
            continue
        try:
            # remove any parentheses, e.g. from 'dask-scheduler   1/1     Running     19 (9m22s ago)   3h20m'
            line = remove_parentheses(line)

            # Remove empty entries from list (caused by multiple \t)
            _l = re.sub(' +', ' ', line)
            _l = [_f for _f in _l.split(' ') if _f]
            # NAME READY STATUS RESTARTS AGE
            if first_line == []:
                first_line = _l[1:]
            else:
                dictionary[_l[0]] = {}
                for i in range(len(_l[1:])):
                    dictionary[_l[0]][first_line[i]] = _l[1:][i]

        except Exception:
            base_logger.warning("unexpected format of utility output: %s", line)

    return dictionary


def open_file(filename, mode):
    """
    Open and return a file pointer for the given mode.
    Note: the caller needs to close the file.

    :param filename: file name (string).
    :param mode: file mode (character).
    :raises PilotException: FileHandlingFailure.
    :return: file pointer.
    """

    try:
        _file = open(filename, mode)
    except IOError as exc:
        _file = None
        base_logger.warning('caught exception: %s', exc)
        # raise FileHandlingFailure(exc)

    return _file


def write_json(filename, data, sort_keys=True, indent=4, separators=(',', ': ')):
    """
    Write the dictionary to a JSON file.

    :param filename: file name (string).
    :param data: object to be written to file (dictionary or list).
    :param sort_keys: should entries be sorted? (boolean).
    :param indent: indentation level, default 4 (int).
    :param separators: field separators (default (',', ': ') for dictionaries, use e.g. (',\n') for lists) (tuple)
    :raises PilotException: FileHandlingFailure.
    :return: status (boolean).
    """

    status = False

    try:
        with open(filename, 'w') as _fh:
            dumpjson(data, _fh, sort_keys=sort_keys, indent=indent, separators=separators)
    except IOError as exc:
        # raise FileHandlingFailure(exc)
        base_logger.warning('caught exception: %s', exc)
    else:
        status = True

    return status


def write_file(path, contents, mute=True, mode='w'):
    """
    Write the given contents to a file.

    :param path: full path for file (string).
    :param contents: file contents (object).
    :param mute: boolean to control stdout info message.
    :param mode: file mode (e.g. 'w', 'r', 'a', 'wb', 'rb') (string).
    :raises PilotException: FileHandlingFailure.
    :return: True if successful, otherwise False.
    """

    status = False

    _file = open_file(path, mode)
    if _file:
        try:
            _file.write(contents)
        except IOError as exc:
            base_logger.warning('caught exception: %s', exc)
            # raise FileHandlingFailure(exc)
        else:
            status = True
        _file.close()

    if not mute:
        if 'w' in mode:
            base_logger.info('created file: %s', path)
        if 'a' in mode:
            base_logger.info('appended file: %s', path)

    return status


def get_pv_yaml(namespace=None, user_id=None, nfs_server=None):
    """
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param nfs_server: NFS server IP (string).
    :return: yaml (string).
    """

    if not namespace:
        base_logger.warning('namespace must be set')
        return ""
    if not user_id:
        base_logger.warning('user id must be set')
        return ""
    if not nfs_server:
        base_logger.warning('NFS server IP must be set')
        return ""

    yaml = """
apiVersion: v1
kind: PersistentVolume
metadata:
  name: fileserver-CHANGE_USERID
  namespace: CHANGE_NAMESPACE
spec:
  capacity:
    storage: 200Gi
  accessModes:
    - ReadWriteMany
  nfs:
    server: CHANGE_NFSSERVER
    path: "/vol1"
"""

    yaml = yaml.replace('CHANGE_USERID', user_id)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_NFSSERVER', nfs_server)

    return yaml


def get_pvc_yaml(namespace=None, user_id=None, nfs_server=None):
    """

    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param nfs_server: Not used (string).
    :return: yaml (string).
    """

    if not namespace:
        base_logger.warning('namespace must be set')
        return ""
    if not user_id:
        base_logger.warning('user id must be set')
        return ""

    yaml = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: fileserver-claim
  namespace: CHANGE_NAMESPACE
spec:
  # Specify "" as the storageClassName so it matches the PersistentVolume's StorageClass.
  # A nil storageClassName value uses the default StorageClass. For details, see
  # https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class-1
  accessModes:
  - ReadWriteMany
  storageClassName: ""
  volumeName: fileserver-CHANGE_USERID
  resources:
    requests:
      storage: 200Gi
"""

    yaml = yaml.replace('CHANGE_USERID', user_id)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)

    return yaml


def get_service_yaml(namespace=None, name=None, port=80, targetport=8786):
    """
    Return the yaml for the dask-scheduler load balancer service.

    :param namespace: namespace (string).
    :param name: service name (string).
    :param port: port (int).
    :param targetport: target port (default dask scheduler port, 8786) (int).
    :return: yaml (string).
    """

    if not namespace:
        base_logger.warning('namespace must be set')
        return ""
    if not name:
        base_logger.warning('service name must be set')
        return ""

    yaml = """
apiVersion: v1
kind: Service
metadata:
  name: CHANGE_SERVICENAME
  namespace: CHANGE_NAMESPACE
  labels:
    name: CHANGE_SERVICENAME
spec:
  type: LoadBalancer
  selector:
    name: CHANGE_SERVICENAME
  ports:
  - protocol: TCP
    port: CHANGE_PORT
    targetPort: CHANGE_TARGETPORT
    name: http
"""

    yaml = yaml.replace('CHANGE_SERVICENAME', name)
    yaml = yaml.replace('CHANGE_PORT', str(port))
    yaml = yaml.replace('CHANGE_TARGETPORT', str(targetport))
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)

    return yaml


def get_scheduler_yaml(image_source=None, nfs_path=None, namespace=None, user_id=None, port=8786, password=None):
    """
    Return the yaml for the Dask scheduler for a given image and the path to the shared file system.

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param port: optional container port (int).
    :param password: (not used).
    :return: yaml (string).
    """

    if not image_source:
        base_logger.warning('image source must be set')
        return ""
    if not nfs_path:
        base_logger.warning('nfs path must be set')
        return ""
    if not namespace:
        base_logger.warning('namespace must be set')
        return ""
    if not user_id:
        base_logger.warning('user id must be set')
        return ""

    yaml = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: dask-scheduler
  namespace: CHANGE_NAMESPACE
  labels:
    app: dask-scheduler
spec:
  replicas: 1
  selector:
    matchLabels:
      run: dask-scheduler
  template:
    metadata:
      labels:
        run: dask-scheduler
    spec:
      securityContext:
        runAsUser: 0
      containers:
      - name: dask-scheduler
        image: CHANGE_IMAGE_SOURCE
        volumeMounts:
        - mountPath: CHANGE_NFS_PATH
          name: fileserver-CHANGE_USERID
      volumes:
      - name: fileserver-CHANGE_USERID
        persistentVolumeClaim:
          claimName: fileserver-claim
          readOnly: false
"""

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_USERID', user_id)

    return yaml


def get_jupyterlab_yaml(image_source=None, nfs_path=None, namespace=None, user_id=None, port=8888, password=None):
    """
    Return the yaml for jupyterlab for a given image and the path to the shared file system.

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param password: jupyterlab login password (string).
    :return: yaml (string).
    """

    if not image_source:
        base_logger.warning('image source must be set')
        return ""
    if not nfs_path:
        base_logger.warning('nfs path must be set')
        return ""
    if not namespace:
        base_logger.warning('namespace must be set')
        return ""
    if not user_id:
        base_logger.warning('user id must be set')
        return ""
    if not password:
        base_logger.warning('password must be set')
        return ""

    yaml = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jupyterlab
  namespace: CHANGE_NAMESPACE
  labels:
    name: jupyterlab
spec:
  replicas: 1
  selector:
    matchLabels:
      name: jupyterlab
  template:
    metadata:
      labels:
        name: jupyterlab
    spec:
      securityContext:
        runAsUser: 0
        fsGroup: 0
      containers:
        - name: jupyterlab
          image: CHANGE_IMAGE_SOURCE
          imagePullPolicy: IfNotPresent
          ports:
          - containerPort: CHANGE_PORT
          command:
            - /bin/bash
            - -c
            - |
              start.sh jupyter lab --LabApp.token='CHANGE_PASSWORD' --LabApp.ip='0.0.0.0' --LabApp.allow_root=True
          volumeMounts:
            - name: fileserver-CHANGE_USERID
              mountPath: CHANGE_NFS_PATH
      restartPolicy: Always
      volumes:
      - name: fileserver-CHANGE_USERID
        persistentVolumeClaim:
          claimName: fileserver-claim
"""
    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_PORT', str(port))
    yaml = yaml.replace('CHANGE_USERID', user_id)
    yaml = yaml.replace('CHANGE_PASSWORD', password)

    return yaml


def get_worker_yaml(image_source=None, nfs_path=None, scheduler_ip=None, worker_name=None, namespace=None, user_id=None):
    """
    Return the yaml for the Dask worker for a given image, path to the shared file system and Dask scheduler IP.

    Note: do not generate random worker names, use predictable worker_name; e.g. dask-worker-00001 etc
    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param scheduler_ip: dask scheduler IP (string).
    :param worker_name: dask worker name (string).
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :return: yaml (string).
    """

    if not image_source:
        base_logger.warning('image source must be set')
        return ""
    if not nfs_path:
        base_logger.warning('nfs path must be set')
        return ""
    if not scheduler_ip:
        base_logger.warning('dask scheduler IP must be set')
        return ""
    if not worker_name:
        base_logger.warning('dask worker name must be set')
        return ""
    if not namespace:
        base_logger.warning('namespace must be set')
        return ""
    if not user_id:
        base_logger.warning('user id must be set')
        return ""

# image_source=palnilsson/dask-worker:latest
# scheduler_ip=e.g. tcp://10.8.2.3:8786
    yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: CHANGE_WORKER_NAME
  namespace: CHANGE_NAMESPACE
spec:
  restartPolicy: Never
  containers:
  - name: CHANGE_WORKER_NAME
    image: CHANGE_IMAGE_SOURCE
    env:
    - name: DASK_SCHEDULER_IP
      value: "CHANGE_DASK_SCHEDULER_IP"
    - name: DASK_SHARED_FILESYSTEM_PATH
      value: CHANGE_NFS_PATH
    volumeMounts:
    - mountPath: CHANGE_NFS_PATH
      name: fileserver-CHANGE_USERID
  volumes:
  - name: fileserver-CHANGE_USERID
    persistentVolumeClaim:
      claimName: fileserver-claim
      readOnly: false
"""

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_DASK_SCHEDULER_IP', scheduler_ip)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_WORKER_NAME', worker_name)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_USERID', user_id)

    return yaml


def get_remote_cleanup_yaml(image_source=None, nfs_path=None, namespace=None, workdir=None, user_id=None):
    """
    Return the yaml for the remote clean-up pod with the given image and the path to the shared file system.

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param namespace: namespace (string).
    :param workdir: remote working directory to be deleted (string).
    :param user_id: user id (string).
    :return: yaml (string).
    """

    if not image_source:
        base_logger.warning('image source must be set')
        return ""
    if not nfs_path:
        base_logger.warning('nfs path must be set')
        return ""
    if not namespace:
        base_logger.warning('namespace must be set')
        return ""
    if not workdir:
        base_logger.warning('workdir must be set')
        return ""
    if not user_id:
        base_logger.warning('user id must be set')
        return ""

    yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: remote-cleanup
  namespace: CHANGE_NAMESPACE
spec:
  restartPolicy: Never
  containers:
  - name: remote-cleanup
    image: CHANGE_IMAGE_SOURCE
    env:
    - name: WORKDIR
      value: "CHANGE_WORKDIR"
    volumeMounts:
    - mountPath: CHANGE_NFS_PATH
      name: fileserver-CHANGE_USERID
  volumes:
  - name: fileserver-CHANGE_USERID
    persistentVolumeClaim:
      claimName: fileserver-claim
      readOnly: false
"""

    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_WORKDIR', workdir)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_USERID', user_id)

    return yaml


def get_pilot_yaml(pod_name='pilot-image', image_source=None, nfs_path=None, namespace=None, user_id=None, workflow=None, queue=None, lifetime=None, cert_dir=None, proxy=None, workdir=None, config=None):
    """
    Return the yaml for the pilot.

    :param image_source: image source (string).
    :param nfs_path: NFS path (string).
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param workflow: pilot workflow (stager or generic) (string).
    :param queue: PanDA queue name (string).
    :param lifetime: maximum pilot lifetime (seconds) (int).
    :param cert_dir: grid certificate directory (string).
    :param proxy: path to grid proxy (string).
    :param workdir: remote work directory (string).
    :param config: pilot config path (string).
    :return: yaml (string).
    """

    yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: CHANGE_POD_NAME
  namespace: CHANGE_NAMESPACE
spec:
  securityContext:
    runAsUser: CHANGE_PODUSERID
    runAsGroup: CHANGE_PODGROUPID
  restartPolicy: Never
  hostNetwork: true
  containers:
  - name: pilot
    image: CHANGE_IMAGE_SOURCE
    env:
    - name: PILOT_WORKFLOW
      value: "CHANGE_WORKFLOW"
    - name: DASK_SHARED_FILESYSTEM_PATH
      value: "CHANGE_NFS_PATH"
    - name: PILOT_QUEUE
      value: "CHANGE_QUEUE"
    - name: PILOT_LIFETIME
      value: "CHANGE_LIFETIME"
    - name: PILOT_WORKDIR
      value: "CHANGE_WORKDIR"
    - name: PILOT_JOB_LABEL
      value: user
    - name: PILOT_USER
      value: atlas
    - name: X509_CERT_DIR
      value: CHANGE_CERT
    - name: X509_USER_PROXY
      value: CHANGE_PROXY
    - name: HARVESTER_PILOT_CONFIG
      value: CHANGE_CONFIGDIR
    volumeMounts:
    - mountPath: CHANGE_NFS_PATH
      name: fileserver-CHANGE_USERID
  volumes:
#  - name: html
#    emptyDir: {}
  - name: fileserver-CHANGE_USERID
    persistentVolumeClaim:
      claimName: fileserver-claim
      readOnly: false
"""

    yaml = yaml.replace('CHANGE_PODUSERID', pilotpoduserid)
    yaml = yaml.replace('CHANGE_PODGROUPID', pilotpodgroupid)
    yaml = yaml.replace('CHANGE_POD_NAME', pod_name)
    yaml = yaml.replace('CHANGE_IMAGE_SOURCE', image_source)
    yaml = yaml.replace('CHANGE_NFS_PATH', nfs_path)
    yaml = yaml.replace('CHANGE_NAMESPACE', namespace)
    yaml = yaml.replace('CHANGE_USERID', user_id)
    yaml = yaml.replace('CHANGE_WORKFLOW', workflow)
    yaml = yaml.replace('CHANGE_QUEUE', queue)
    yaml = yaml.replace('CHANGE_LIFETIME', str(lifetime))
    yaml = yaml.replace('CHANGE_WORKDIR', workdir)
    yaml = yaml.replace('CHANGE_CONFIGDIR', config)
    yaml = yaml.replace('CHANGE_CERT', cert_dir)
    yaml = yaml.replace('CHANGE_PROXY', proxy)

    return yaml


def get_scheduler_info(timeout=480, namespace=None):
    """
    Wait for the scheduler to start, then grab the scheduler IP from the stdout and its proper pod name.

    :param timeout: time-out (integer).
    :param namespace: namespace (string).
    :return: scheduler IP (string), pod name (string), stderr (string).
    """

    scheduler_ip = ""

    base_logger.debug('get_scheduler_info called')
    podname = get_pod_name(namespace=namespace, pattern=r'(dask\-scheduler\-.+)')
    base_logger.debug(f'calling wait_until_deployment for name={podname}')
    status, _, stderr = wait_until_deployment(name=podname, state='Running', timeout=300, namespace=namespace, deployment=False)
    if not status:
        return scheduler_ip, podname, stderr

    starttime = time.time()
    now = starttime
    _sleep = 5  # sleeping time between attempts
    first = True
    while now - starttime < timeout:
        # get the scheduler stdout
        status, stdout, stderr = kubectl_logs(pod=podname, namespace=namespace)
        if not status or not stdout:
            base_logger.warning('failed to extract scheduler IP from kubectl logs command: %s', stderr)
            return scheduler_ip, podname, stderr

        pattern = r'tcp://[0-9]+(?:\.[0-9]+){3}:[0-9]+'
        for line in stdout.split('\n'):
            if "Scheduler at:" in line:
                _ip = re.findall(pattern, line)
                if _ip:
                    scheduler_ip = _ip[0]
                    break

        if scheduler_ip:
            break
        else:
            # IP has not yet been extracted, wait longer and try again
            if first:
                base_logger.info('sleeping until scheduler IP is known (timeout=%d s)', timeout)
                first = False
            time.sleep(_sleep)
            now = time.time()

    return scheduler_ip, podname, ''


def get_jupyterlab_info(timeout=300, namespace=None):
    """
    Wait for the jupyterlab pod to start and its proper pod name.

    :param timeout: time-out (integer).
    :param namespace: namespace (string).
    :return: unused string (string), pod name (string), stderr (string).
    """

    podname = get_pod_name(namespace=namespace, pattern=r'(jupyterlab\-.+)')
    base_logger.debug(f'calling wait_until_deployment for name={podname}')
    _, _, stderr = wait_until_deployment(name=podname, state='Running', timeout=300, namespace=namespace, deployment=False)
    if stderr:
        return '', podname, stderr

    starttime = time.time()
    now = starttime
    _sleep = 5  # sleeping time between attempts
    first = True
    started = False
    while now - starttime < timeout:
        # get the scheduler stdout
        status, stdout, stderr = kubectl_logs(pod=podname, namespace=namespace)
        if not status or not stdout:
            base_logger.warning('jupyterlab pod stdout:\n%s', stdout)
            base_logger.warning('jupyterlab pod failed to start: %s', stderr)
            return '', podname, stderr

        for line in stdout.split('\n'):
            if "Jupyter Server" in line and 'is running at:' in line:
                base_logger.info('jupyter server is running')
                started = True
                break

        if started:
            break

        # IP has not yet been extracted, wait longer and try again
        if first:
            base_logger.info('sleeping until jupyter server has started (timeout=%d s)', timeout)
            first = False
        time.sleep(_sleep)
        now = time.time()

    if not started:
        stderr = 'Jupyter server did not start'
    return '', podname, stderr


def deploy_workers(scheduler_ip, _nworkers, yaml_files, namespace, user_id, imagename, mountpath, workdir, pandaid, taskid):
    """
    Deploy the worker pods and return a dictionary with the worker info.

    worker_info = { worker_name_%d: yaml_path_%d, .. }

    :param scheduler_ip: dask scheduler IP (string).
    :param _nworkers: number of workers (int).
    :param yaml_files: yaml files dictionary.
    :param namespace: namespace (string).
    :param user_id: user id (string).
    :param imagename: image name (string).
    :param mountpath: FS mount path (string).
    :param workdir: path to working directory (string).
    :param pandaid: panda id (int).
    :param taskid: task id (int).
    :return: worker info dictionary, stderr (dictionary, string).
    """

    worker_info = {}
    for _iworker in range(_nworkers):

        worker_name = f'dask-worker-{_iworker}-{pandaid}'
        worker_path = os.path.join(workdir, yaml_files.get('dask-worker') % (taskid, _iworker))
        worker_info[worker_name] = worker_path

        # create worker yaml
        worker_yaml = get_worker_yaml(image_source=imagename,
                                      nfs_path=mountpath,
                                      scheduler_ip=scheduler_ip,
                                      worker_name=worker_name,
                                      namespace=namespace,
                                      user_id=user_id)
        status = write_file(worker_path, worker_yaml)
        if not status:
            stderr = 'cannot continue since yaml file could not be created'
            base_logger.warning(stderr)
            return None, stderr

        # start the worker pod
        status, _, stderr = kubectl_create(filename=worker_path)
        if not status:
            return None, stderr

        base_logger.info(f'deployed {worker_name} pod')

    return worker_info, ''


def await_worker_deployment(namespace, scheduler_pod_name='', jupyter_pod_name='', timeout=300):
    """
    Wait for all workers to start running.

    pod_infp = { pod_name : { start_time: .., status: ..} }

    :param namespace: namespace (string).
    :param scheduler_pod_name: pod name for scheduler (string).
    :param timeout: optional time-out (int).
    :return: True if all pods end up in Running state (Boolean), pod info (dictionary).
    """

    running_workers = []
    starttime = time.time()
    now = starttime
    _sleep = 5
    processing = True
    status = True
    pods = {}
    counter = 0
    while processing and (now - starttime < timeout):

        if counter == 0:
            base_logger.debug(f'awaiting worker start since {time.time() - starttime} seconds')
        # get the full pod info dictionary - note: not good if MANY workers
        status, stdout, stderr = kubectl_execute(cmd='get pods', namespace=namespace)
        if not status:
            break

        # convert command output to a dictionary
        dictionary = _convert_to_dict(stdout)

        # get list of workers and get rid of the scheduler and workers that are already known to be running
        workers_list = list(dictionary.keys())
        for pod_name in [scheduler_pod_name, jupyter_pod_name]:
            if pod_name == 'not_used' or not pod_name:
                continue
            if pod_name in workers_list:
                workers_list.remove(pod_name)
            else:
                if len(workers_list) < 10:
                    base_logger.debug(f'{pod_name} not in workers list={workers_list}')

        for running_worker in running_workers:
            if running_worker in workers_list:
                workers_list.remove(running_worker)
            else:
                if len(workers_list) < 10:
                    base_logger.debug(f'{running_worker} not in workers list={workers_list}')

        # check the states
        if counter == 0:
            base_logger.debug(f'counter={counter} workers_list={workers_list}')

        # is pilot pod running? if so, remove it from the list

        for worker_name in workers_list:
            # is the worker in Running state?
            try:
                state = dictionary[worker_name]['STATUS']
            except KeyError as exc:
                stderr = 'caught exception: {exc}'
                base_logger.warning(stderr)
            else:
                if state == 'Running' or state == 'Completed' or state == 'Error':
                    running_workers.append(worker_name)
                    #base_logger.debug(f'{worker_name} is in state \"{state}\"')
                if counter == 0:
                    base_logger.debug(f'{worker_name} is in state \"{state}\"')

                pod_info = {
                    'start_time': time.time() if state == 'Running' else '',
                    'status': state
                }
                pods[worker_name] = pod_info

        if len(running_workers) == len(list(dictionary.keys())) - 2:
            processing = False
        else:
            time.sleep(_sleep)
            now = time.time()
            counter += 1
            if counter >= 10:
                counter = 0
                base_logger.debug('number of workers: {len(running_workers)}')

    base_logger.debug(f'number of dask workers: {len(running_workers)}')

    return status, pods


def mkdirs(workdir, chmod=0o770):
    """
    Create a directory.
    Perform a chmod if set.

    :param workdir: Full path to the directory to be created
    :param chmod: chmod code (default 0770) (octal).
    :raises PilotException: MKDirFailure.
    :return:
    """

    try:
        os.makedirs(workdir)
        if chmod:
            os.chmod(workdir, chmod)
    except Exception as exc:
        raise exc
    else:
        if os.path.exists(workdir):
            base_logger.info(f'created directory {workdir}')
        else:
            raise OSError(f'failed to create directory {workdir}')


def to_dict(job_spec):
    """
    Create a job spec dictionary out of a JobSpec object.

    :param job_spec: JobSpec object.
    :return: job spec dictionary.
    """

    job_spec_dict = {}

#    try:
#        job_spec_dict['PandaID'] = job_spec.PandaID
#        job_spec_dict['taskID'] = job_spec.taskID
#        job_spec_dict['jobsetID'] = job_spec.jobParams['jobsetID']
#    job_spec_dict[''] = job_spec.
#    job_spec_dict[''] = job_spec.
#    job_spec_dict[''] = job_spec.
#    job_spec_dict[''] = job_spec.
#    job_spec_dict[''] = job_spec.
#    job_spec_dict[''] = job_spec.
#
#    'logGUID': log_guid,
#    'cmtConfig': 'x86_64-slc6-gcc48-opt',
#    'prodDBlocks': 'user.mlassnig:user.mlassnig.pilot.test.single.hits',
#    'dispatchDBlockTokenForOut': 'NULL,NULL',
#    'destinationDBlockToken': 'NULL,NULL',
#    'destinationSE': 'AGLT2_TEST',
#    'realDatasets': job_name,
#    'prodUserID': 'no_one',
#    'GUID': guid,
#    'realDatasetsIn': 'user.mlassnig:user.mlassnig.pilot.test.single.hits',
#    'nSent': 0,
#    'cloud': 'US',
#    'StatusCode': 0,
#    'homepackage': 'AtlasProduction/20.1.4.14',
#    'inFiles': 'HITS.06828093._000096.pool.root.1',
#    'processingType': 'pilot-ptest',
#    'ddmEndPointOut': 'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
#    'fsize': '94834717',
#    'fileDestinationSE': 'AGLT2_TEST,AGLT2_TEST',
#    'scopeOut': 'panda',
#    'minRamCount': 0,
#    'jobDefinitionID': 7932,
#    'maxWalltime': 'NULL',
#    'scopeLog': 'panda',
#    'transformation': 'Reco_tf.py',
#    'maxDiskCount': 0,
#    'coreCount': 1,
#    'prodDBlockToken': 'NULL',
#    'transferType': 'NULL',
#    'destinationDblock': job_name,
#    'dispatchDBlockToken': 'NULL',
#    'jobPars': '--maxEvents=1 --inputHITSFile HITS.06828093._000096.pool.root.1 --outputRDOFile RDO_%s.root' % job_name,
#    'attemptNr': 0,
#    'swRelease': 'Atlas-20.1.4',
#    'nucleus': 'NULL',
#    'maxCpuCount': 0,
#    'outFiles': 'RDO_%s.root,%s.job.log.tgz' % (job_name, job_name),
#    'currentPriority': 1000,
#    'scopeIn': 'mc15_13TeV',
#    'sourceSite': 'NULL',
#    'dispatchDblock': 'NULL',
#    'prodSourceLabel': 'ptest',
#    'checksum': 'ad:5d000974',
#    'jobName': job_name,
#    'ddmEndPointIn': 'UTA_SWT2_DATADISK',
#    'logFile': '%s.job.log.tgz' % job_name}

    try:
        job_params = job_spec.get_job_params(False)
        job_spec_dict = {job_spec.PandaID: job_params}
    except Exception as exc:
        base_logger.warning(f'failed to create job spec dictionary: {exc}')

    return job_spec_dict


def extract_pod_info(namespace):
    """
    Extract the actual namespace, taskid, mode, scheduler, session and pilot pod names encoded in the work spec namespace variable.
    The 'session' would typically be jupyterlab.

    :param namespace: encoded name space (string).
    :return: actual name space (str), task id (str), interactive mode (str), scheduler pod name (str), session pod name (str), pilot pod name (str).
    """

    _namespace = ''
    _taskid = ''
    _mode = ''
    _scheduler_pod_name = ''
    _session_pod_name = ''
    _pilot_pod_name = ''
    pattern = r'namespace\=(.+)\:taskid\=(.+)\:mode\=(.+)\:dask\-scheduler\_pod\_name\=(.+)\:session\_pod\_name\=(.+)\:pilot\_pod\_name\=(.+)'
    try:
        info = re.findall(pattern, namespace)
        _namespace = info[0][0]
        _taskid = info[0][1]
        _mode = info[0][2]
        _scheduler_pod_name = info[0][3]
        _session_pod_name = info[0][4]
        _pilot_pod_name = info[0][5]
    except Exception as exc:
        base_logger(f'failed to extract pod info from namespace={namespace}: {exc}')

    return _namespace, _taskid, _mode, _scheduler_pod_name, _session_pod_name, _pilot_pod_name


def remove_local_dir(directory):
    """
    Remove the given local directory.

    :param directory: directory name (string).
    :return:
    """

    try:
        rmtree(directory)
    except OSError as exc:
        base_logger.warning(exc)
    else:
        base_logger.info(f'removed local directory {directory}')
