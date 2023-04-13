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

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestermisc import dask_utils

# logger
base_logger = core_utils.setup_logger('dask_submitter_base')

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
ERROR_PILOT = 10

# submitter for Dask - one instance per panda job
class DaskSubmitterBase(object):

    _nworkers = None
    _namespace = None
    _userid = None
    _mountpath = None
    _ispvc = None
    _ispv = None
    _username = None
    _password = None
    _interactive_mode = None
    _session_type = None
    _workdir = None
    _pilot_config = None
    _remote_workdir = None
    _nfs_server = None
    _files = None
    _images = None
    _podnames = None
    _ports = None
    _pandaid = None
    _workspec = None
    _queuename = None
    _remote_proxy = None
    _cert_dir = '/etc/grid-security/certificates'

    # constructor
    def __init__(self, **kwargs):

        self._nworkers = kwargs.get('nworkers', 1)
        self._namespace = kwargs.get('namespace')
        self._userid = kwargs.get('userid')
        self._mountpath = '/mnt/dask'
        self._ispvc = False  # set when PVC is successfully created
        self._ispv = False  # set when PV is successfully created
        self._username = kwargs.get('username')
        self._password = kwargs.get('password')
        self._interactive_mode = kwargs.get('interactive_mode', True)
        self._session_type = kwargs.get('session_type', 'jupyterlab')
        self._local_workdir = kwargs.get('local_workdir')
        self._remote_workdir = kwargs.get('remote_workdir')
        self._remote_workdir = self._remote_workdir.split(':')[-1] if ':' in self._remote_workdir else self._remote_workdir
        self._pilot_config = kwargs.get('pilot_config')
        self._pilot_config = self._pilot_config.split(':')[-1] if ':' in self._pilot_config else self._pilot_config
        self._nfs_server = kwargs.get('nfs_server', '10.173.46.194')
        self._pandaid = kwargs.get('pandaid')
        self._taskid = kwargs.get('taskid')
        self._workspec = kwargs.get('workspec')
        self._queuename = kwargs.get('queuename')
        self._remote_proxy = kwargs.get('remote_proxy')

        self._files = {  # pandaid will be added (amd dask worker id in the case of 'dask-worker')
            'dask-scheduler-service': '%d-dask-scheduler-service.yaml',
            'dask-scheduler': '%d-dask-scheduler-deployment.yaml',
            'dask-worker': '%d-dask-worker-deployment-%d.yaml',
            'dask-pilot': '%d-dask-pilot-deployment.yaml',
            'pilot-image': '%d-pilot-deployment.yaml',
            'jupyterlab-service': '%d-jupyterlab-service.yaml',
            'jupyterlab': '%d-jupyterlab-deployment.yaml',
            'namespace': '%d-namespace.json',
            'pvc': '%d-pvc.yaml',
            'pv': '%d-pv.yaml',
            'remote-cleanup': '%d-remote-cleanup.yaml',
        }

        self._images = {
            'dask-scheduler': 'europe-west1-docker.pkg.dev/gke-dev-311213/dask-images/dask-scheduler:latest',
            'dask-worker': 'europe-west1-docker.pkg.dev/gke-dev-311213/dask-images/dask-worker:latest',
            'pilot': 'europe-west1-docker.pkg.dev/gke-dev-311213/dask-images/pilot-image:latest',
            'jupyterlab': 'europe-west1-docker.pkg.dev/gke-dev-311213/dask-images/datascience-notebook:latest',
            'remote-cleanup': 'europe-west1-docker.pkg.dev/gke-dev-311213/dask-images/remote-cleanup:latest',
        }

        self._podnames = {
            'dask-scheduler-service': 'dask-scheduler',
            'dask-scheduler': 'dask-scheduler',
            'dask-worker': 'dask-worker',
            'pilot': 'pilot',
            'pilot-image': 'pilot-image',
            'jupyterlab-service': 'jupyterlab',
            'jupyterlab': 'jupyterlab',
            'remote-cleanup': 'remote-cleanup',
        }

        # { name: [port, targetPort], .. }
        self._ports = {'dask-scheduler-service': [80, 8786],
                       'jupyterlab-service': [80, 8888]}

    def get_ports(self, servicename):
        """
        Return the port and targetport for a given server name.

        Format: [port, targetPort]

        :param servicename: service name (string),
        :return: ports (list).
        """

        return self._ports.get(servicename, 'unknown')

    def get_userid(self):
        """
        Return the user id.

        :return: user id (string).
        """

        return self._userid

    def get_namespace(self):
        """
        Return the namespace.

        namespace = single-user-<user id>.

        :return: namespace (string).
        """

        return self._namespace

    def create_namespace(self):
        """
        Create the random namespace.

        :return: True if successful, stderr (Boolean, string).
        """

        namespace_filename = os.path.join(self._local_workdir, self._files.get('namespace') % self._pandaid)
        base_logger.debug(f'namespace_filename={namespace_filename}, namespace={self._namespace}')
        return dask_utils.create_namespace(self._namespace, namespace_filename)

    def create_pvcpv(self, name='pvc'):
        """
        Create the PVC or PV.

        :param name: 'pvc' or 'pv' (string).
        :return: True if successful (Boolean), stderr (string).
        """

        if name not in ['pvc', 'pv']:
            stderr = 'unknown PVC/PC name: %s', name
            base_logger.warning(stderr)
            return False, stderr

        # create the yaml file
        path = os.path.join(os.path.join(self._local_workdir, self._files.get(name) % self._pandaid))
        func = dask_utils.get_pvc_yaml if name == 'pvc' else dask_utils.get_pv_yaml
        yaml = func(namespace=self._namespace, user_id=self._userid, nfs_server=self._nfs_server)
        status = dask_utils.write_file(path, yaml)
        if not status:
            return False, 'write_file failed for file %s' % path

        # create the PVC/PV
        status, _, stderr = dask_utils.kubectl_create(filename=path)
        if name == 'pvc':
            self._ispvc = status
        elif name == 'pv':
            self._ispv = status

        return status, stderr

    def deploy_service_pod(self, name):
        """
        Deploy the dask scheduler.

        :param name: service name (string).
        :return: stderr (string).
        """

        fname = self._files.get(name) % self._pandaid
        if fname == 'unknown':
            stderr = 'unknown file name for %s yaml' % name
            base_logger.warning(stderr)
            return stderr
        image = self._images.get(name, 'unknown')
        if image == 'unknown':
            stderr = 'unknown image for %s pod' % name
            base_logger.warning(stderr)
            return stderr

        # create yaml
        name += '-service'
        func = dask_utils.get_scheduler_yaml if name == 'dask-scheduler-service' else dask_utils.get_jupyterlab_yaml
        path = os.path.join(self._local_workdir, fname)
        yaml = func(image_source=image,
                    nfs_path=self._mountpath,
                    namespace=self._namespace,
                    user_id=self._userid,
                    port=self.get_ports(name)[1],
                    password=self._password)
        status = dask_utils.write_file(path, yaml, mute=False)
        if not status:
            stderr = 'cannot continue since file %s could not be created' % path
            base_logger.warning(stderr)
            return stderr

        # start the dask scheduler pod
        status, _, stderr = dask_utils.kubectl_create(filename=path)
        if not status:
            return stderr

        return ""

    def get_service_info(self, service):
        """
        Return the relevant IP and pod name for the given service (when available).

        :param service: service name (string).
        :return: IP number (string), pod name (string), stderr (string).
        """

        func = dask_utils.get_scheduler_info if service == 'dask-scheduler' else dask_utils.get_jupyterlab_info
        return func(namespace=self._namespace)

    def deploy_dask_workers(self, scheduler_ip='', scheduler_pod_name='', jupyter_pod_name=''):
        """
        Deploy all dask workers.

        :param scheduler_ip: dask scheduler IP (string).
        :param scheduler_pod_name: pod name for scheduler (string).
        :param optional jupyter_pod_name: pod name for jupyterlab (string).
        :return: True if successful, stderr (Boolean, string)
        """

        worker_info, stderr = dask_utils.deploy_workers(scheduler_ip,
                                                        self._nworkers,
                                                        self._files,
                                                        self._namespace,
                                                        self._userid,
                                                        self._images.get('dask-worker', 'unknown'),
                                                        self._mountpath,
                                                        self._local_workdir,
                                                        self._pandaid)
        if not worker_info:
            base_logger.warning('failed to deploy workers: %s', stderr)
            return False, stderr

        # wait for the worker pods to start
        # (send any scheduler and jupyter pod name to function so they can be removed from a query)
        #try:
        #    status = dask_utils.await_worker_deployment(self._namespace,
        #                                               scheduler_pod_name=scheduler_pod_name,
        #                                               jupyter_pod_name=jupyter_pod_name)
        #except Exception as exc:
        #    stderr = 'caught exception: %s', exc
        #    base_logger.warning(stderr)
        #   status = False
        status = True if not stderr else False
        return status, stderr

    def deploy_cleanup(self, remote_workdir):
        """
        Deploy the pilot pod.
        The method will return when the pod is in state 'Terminated'.

        :return: True if successful (Boolean), [None], stderr (string).
        """

        # create cleanup yaml
        path = os.path.join(self._local_workdir, self._files.get('remote-cleanup') % self._pandaid)
        yaml = dask_utils.get_remote_cleanup_yaml(image_source=self._images.get('remote-cleanup', 'unknown'),
                                                  nfs_path=self._mountpath,
                                                  namespace=self._namespace,
                                                  workdir=remote_workdir,
                                                  user_id=self._userid)
        status = dask_utils.write_file(path, yaml, mute=False)
        if not status:
            stderr = 'cannot continue since remote-cleanup yaml file could not be created'
            base_logger.warning(stderr)
            return False, stderr

        # start the remote-cleanup pod
        status, _, stderr = dask_utils.kubectl_create(filename=path)
        if not status:
            base_logger.warning(f'failed to create remote-cleanup pod for remote directory {remote_workdir}: %s', stderr)
            return False, stderr
        else:
            base_logger.debug('created remote-cleanup pod (waiting until completed/terminated)')

        return dask_utils.wait_until_deployment(name=self._podnames.get('remote-cleanup', 'unknown'), state='Completed|Terminated', namespace=self._namespace)

    def deploy_pilot(self):
        """
        Deploy the pilot pod.

        :return: True if successful (Boolean), [None], stderr (string).
        """

        # create pilot yaml
        path = os.path.join(self._local_workdir, self._files.get('pilot-image') % self._pandaid)
        yaml = dask_utils.get_pilot_yaml(pod_name=self._podnames.get('pilot'),
                                         image_source=self._images.get('pilot', 'unknown'),
                                         nfs_path=self._mountpath,
                                         namespace=self._namespace,
                                         user_id=self._userid,
                                         workflow=self.get_pilot_workflow(),
                                         queue=self._queuename,
                                         lifetime=100,
                                         cert_dir=self._cert_dir,
                                         proxy=self._remote_proxy,
                                         workdir=self._remote_workdir,
                                         config=self._pilot_config
                                         )
        status = dask_utils.write_file(path, yaml, mute=False)
        if not status or not os.path.exists(path):
            stderr = f'cannot continue since pilot yaml file {path} could not be created'
            base_logger.warning(stderr)
            return False, stderr
        else:
            base_logger.debug(f'created {path}')

        # start the pilot pod
        status, _, stderr = dask_utils.kubectl_create(filename=path)
        if not status:
            base_logger.warning(f'failed to create pilot pod in namespace {self._namespace}: {stderr}')
            return False, stderr
        else:
            base_logger.debug('created pilot pod')

        return True, ""

    def get_pilot_workflow(self):
        """
        Return the proper label for the pilot workflow.

        The function will return workflow='stager' for interactive mode and 'generic' for non-interactive mode.

        :return: 'stager' or 'generic' (string).
        """

        return 'stager' if self._interactive_mode else 'generic'

    def copy_bundle(self):
        """
        Copy bundle (incl. job definition).

        :return: True if successful (Boolean).
        """

        status = True

        return status

    def get_service_name(self, name):
        """
        Return the proper internal service name.

        :param name: general service name (string).
        :return: internal service name (string).
        """

        return self._podnames.get(name, 'unknown')

    def create_service(self, servicename, port, targetport):
        """
        Create a service yaml and start it.

        :param servicename: service name (string).
        :param port: port (int).
        :param targetport: targetport (int).
        :return: stderr (string)
        """

        _stderr = ''

        path = os.path.join(self._local_workdir, self._files.get(servicename) % self._pandaid)
        yaml = dask_utils.get_service_yaml(namespace=self._namespace,
                                          name=self._podnames.get(servicename, 'unknown'),
                                          port=port,
                                          targetport=targetport)
        status = dask_utils.write_file(path, yaml, mute=False)
        if not status:
            _stderr = 'cannot continue since %s service yaml file could not be created' % servicename
            base_logger.warning(_stderr)
            return _stderr

        # start the service
        status, _, _stderr = dask_utils.kubectl_create(filename=path)
        if not status:
            base_logger.warning('failed to create %s pod: %s', self._podnames.get(servicename, 'unknown'), _stderr)
            return _stderr

        #        status, _external_ip, _stderr = dask_utils.wait_until_deployment(name=self._podnames.get('dask-scheduler-service','unknown'), namespace=self._namespace)

        return _stderr

    def wait_for_service(self, name):
        """
        Wait for a given service deployment to start.

        :param name: service name (string)
        :return: host IP (string), stderr (string).
        """

        _, _ip, _stderr = dask_utils.wait_until_deployment(name=self._podnames.get(name, 'unknown'),
                                                          namespace=self._namespace, service=True)
        return _ip, _stderr

    def install(self, timing):
        """
        Install all services and deploy all pods.

        The service_info dictionary containers service info partially to be returned to the user (external and internal IPs)
        It has the format
          { service: {'external_ip': <ext. ip>, 'internal_ip': <int. ip>, 'pod_name': <pod_name>}, ..}

        :param timing: timing dictionary.
        :return: exit code (int), service_info (dictionary), stderr (string).
        """

        exitcode = 0
        service_info = {}

        # create unique name space
        status, stderr = self.create_namespace()
        if not status:
            stderr = f'failed to create namespace {self.get_namespace()}: {stderr}'
            base_logger.warning(stderr)
            self.cleanup()
            return ERROR_NAMESPACE, {}, stderr
        timing['tnamespace'] = time.time()
        base_logger.info(f'created namespace: {self.get_namespace()}')

        # create PVC and PV
        for name in ['pvc', 'pv']:
            status, stderr = self.create_pvcpv(name=name)
            if not status:
                stderr = f'could not create PVC/PV: {stderr}'
                base_logger.warning(stderr)
                self.cleanup(namespace=self._namespace, user_id=self._userid)
                exitcode = ERROR_PVPVC
                break
        timing['tpvcpv'] = time.time()
        if exitcode:
            return exitcode, {}, stderr
        base_logger.info('created PVC and PV')

        # create the dask scheduler service with a load balancer (the external IP of the load balancer will be made
        # available to the caller)
        # [wait until external IP is available]
        services = ['dask-scheduler', 'jupyterlab']
        for service in services:
            _service = service + '-service'
            ports = self.get_ports(_service)
            stderr = self.create_service(_service, ports[0], ports[1])
            if stderr:
                exitcode = ERROR_CREATESERVICE
                self.cleanup(namespace=self._namespace, user_id=self._userid, pvc=True, pv=True)
                break
        timing['tservices'] = time.time()
        if exitcode:
            return exitcode, {}, stderr

        # start services with load balancers
        for service in services:
            _service = service + '-service'
            _ip, stderr = self.wait_for_service(_service)
            if stderr:
                stderr = f'failed to start load balancer for {_service}: {stderr}'
                base_logger.warning(stderr)
                self.cleanup(namespace=self._namespace, user_id=self._userid, pvc=True, pv=True)
                exitcode = ERROR_LOADBALANCER
                break
            if service not in service_info:
                service_info[service] = {}
            service_info[service]['external_ip'] = _ip
            base_logger.info(f'load balancer for {_service} has external ip={service_info[service].get("external_ip")}')
        timing['tloadbalancers'] = time.time()
        if exitcode:
            return exitcode, {}, stderr

        # deploy the dask scheduler (the scheduler IP will only be available from within the cluster)
        for service in services:
            stderr = self.deploy_service_pod(service)
            if stderr:
                stderr = f'failed to deploy {service} pod: {stderr}'
                base_logger.warning(stderr)
                self.cleanup(namespace=self._namespace, user_id=self._userid, pvc=True, pv=True)
                exitcode = ERROR_DEPLOYMENT
                break
        timing['tdeployments'] = time.time()
        if exitcode:
            return exitcode, {}, stderr

        # get the scheduler and jupyterlab info
        # for the dask scheduler, the internal IP number is needed
        # for jupyterlab, we only need to verify that it started properly
        for service in services:
            base_logger.debug(f'calling get_service_info for service={service}')
            internal_ip, _pod_name, stderr = self.get_service_info(service)
            if stderr:
                stderr = f'{service} pod failed: {stderr}'
                base_logger.warning(stderr)
                self.cleanup(namespace=self._namespace, user_id=self._userid, pvc=True, pv=True)
                exitcode = ERROR_PODFAILURE
                break
            service_info[service]['internal_ip'] = internal_ip
            service_info[service]['pod_name'] = _pod_name
            if internal_ip:
                base_logger.info(f'pod {_pod_name} with internal ip={internal_ip} started correctly')
            else:
                base_logger.info(f'pod {_pod_name} started correctly')
        timing['tserviceinfo'] = time.time()
        if exitcode:
            return exitcode, {}, stderr

        # switch context for the new namespace
        # status = dask_utils.kubectl_execute(cmd='config use-context', namespace=namespace)

        # switch context for the new namespace
        # status = dask_utils.kubectl_execute(cmd='config use-context', namespace='default')

        # store the scheduler pod names, so the monitor can start checking the pod statuses
        self._workspec.namespace = f"namespace={self._namespace}:" \
                                   f"taskid={self._taskid}:" \
                                   f"dask-scheduler_pod_name={service_info['dask-scheduler'].get('pod_name')}:" \
                                   f"session_pod_name={service_info['jupyterlab'].get('pod_name')}:" \
                                   f"pilot_pod_name={self._podnames.get('pilot')}"  # pilot pod not created yet

        # deploy the pilot pod, but do not wait for it to start (will be done by the dask monitor)
        status, stderr = self.deploy_pilot()
        if not status:
            base_logger.warning(stderr)
            self.cleanup(namespace=self._namespace, user_id=self._userid, pvc=True, pv=True)
            return ERROR_PILOT, {}, stderr

        # deploy the worker pods, but do not wait for them to start (will be done by the dask monitor)
        status, stderr = self.deploy_dask_workers(scheduler_ip=service_info['dask-scheduler'].get('internal_ip'),
                                                  scheduler_pod_name=service_info['dask-scheduler'].get('pod_name'),
                                                  jupyter_pod_name=service_info['jupyterlab'].get('pod_name'))
        if not status:
            stderr = f'failed to deploy dask workers: {stderr}'
            base_logger.warning(stderr)
            self.cleanup(namespace=self._namespace, user_id=self._userid, pvc=True, pv=True)
            exitcode = ERROR_DASKWORKER

        timing['tdaskworkers'] = time.time()
        if exitcode:
            return exitcode, {}, stderr
        base_logger.info('deployed all dask-worker pods')

        # return the jupyterlab and dask scheduler IPs to the user in interactive mode
        return exitcode, service_info, stderr

        #######################################

        # time.sleep(30)
        #cmd = f'kubectl logs pilot-image --namespace=single-user-{self._userid}'
        #base_logger.debug(f'executing: {cmd}')
        #ec, stdout, stderr = dask_utils.execute(cmd)
        #base_logger.debug(stdout)

        #if not status:
        #    self.cleanup(namespace=self._namespace, user_id=self._userid, pvc=True, pv=True)
        #    exit(-1)
        #base_logger.info('deployed pilot pod')

        return exitcode, stderr

    def timing_report(self, timing, info=None):
        """
        Display the timing report.

        :param timing: timing dictionary.
        :param info: info string to be prepended to timing report (string).
        :return:
        """

        _info = info if info else ''
        _info += '\n* timing report ****************************************'
        for key in timing:
            _info += '\n%s:\t\t\t%d s' % (key, timing.get(key) - timing.get('t0'))
        _info += '\n----------------------------------'
        _info += '\ntotal time:\t\t\t%d s' % sum((timing[key] - timing['t0']) for key in timing)
        _info += '\n********************************************************'
        base_logger.info(_info)

    def create_cleanup_script(self, workerid):
        """
        Create a clean-up script, useful for interactive sessions (at least in stand-alone mode).
        The script can be executed by the dask sweeper, which should also delete the file.

        :return:
        """

        cmds = '#!/bin/bash\n'
        cmds += f'kubectl delete --all deployments --namespace=single-user-{self._userid}\n'
        cmds += f'kubectl delete --all pods --namespace=single-user-{self._userid}\n'
        cmds += f'kubectl delete --all services --namespace=single-user-{self._userid}\n'
        cmds += 'kubectl patch pvc fileserver-claim -p \'{"metadata":{"finalizers":null}}\' --namespace=single-user-%s\n' % self._userid
        cmds += f'kubectl delete pvc fileserver-claim --namespace=single-user-{self._userid}\n'
        cmds += 'kubectl patch pv fileserver-%s -p \'{"metadata":{"finalizers":null}}\' --namespace=single-user-%s\n' % (self._userid, self._userid)
        cmds += f'kubectl delete pv fileserver-{self._userid} --namespace=single-user-{self._userid}\n'
        cmds += f'kubectl delete namespaces single-user-{self._userid}\n'

        path = os.path.join(self._local_workdir, f'{workerid}-cleanup.sh')
        status = dask_utils.write_file(path, cmds)
        if not status:
            return False, f'write_file failed for file {path}'
        else:
            os.chmod(path, 0o755)

    def cleanup(self, namespace=None, user_id=None, pvc=False, pv=False):
        """
        General cleanup.

        :param namespace: namespace (string).
        :param user_id: user id (string).
        :param pvc: True if PVC was created (Boolean).
        :param pv: True if PV was created (Boolean).
        :return:
        """

        if namespace:
            cmd = f'kubectl delete --all deployments --namespace={namespace}'
            base_logger.debug(f'executing: {cmd}')
            ec, stdout, stderr = dask_utils.execute(cmd)
            base_logger.debug(stdout)

            cmd = f'kubectl delete --all pods --namespace={namespace}'
            base_logger.debug(f'executing: {cmd}')
            ec, stdout, stderr = dask_utils.execute(cmd)
            base_logger.debug(stdout)

            cmd = f'kubectl delete --all services --namespace={namespace}'
            base_logger.debug(f'executing: {cmd}')
            ec, stdout, stderr = dask_utils.execute(cmd)
            base_logger.debug(stdout)

        if pvc:
            cmd = 'kubectl patch pvc fileserver-claim -p \'{\"metadata\": {\"finalizers\": null}}\' --namespace=%s' % namespace
            base_logger.debug(f'executing: {cmd}')
            ec, stdout, stderr = dask_utils.execute(cmd)
            base_logger.debug(stdout)
            cmd = f'kubectl delete pvc fileserver-claim --namespace={namespace}'
            base_logger.debug(f'executing: {cmd}')
            ec, stdout, stderr = dask_utils.execute(cmd)
            base_logger.debug(stdout)

        if pv:
            cmd = 'kubectl patch pv fileserver-%s -p \'{\"metadata\": {\"finalizers\": null}}\'' % user_id
            base_logger.debug(f'executing: {cmd}')
            ec, stdout, stderr = dask_utils.execute(cmd)
            base_logger.debug(stdout)
            cmd = f'kubectl delete pv fileserver-{user_id}'
            base_logger.debug(f'executing: {cmd}')
            ec, stdout, stderr = dask_utils.execute(cmd)
            base_logger.debug(stdout)

        if namespace:
            cmd = f'kubectl delete namespaces {namespace}'
            base_logger.debug(f'executing: {cmd}')
            ec, stdout, stderr = dask_utils.execute(cmd)
            base_logger.debug(stdout)

        # cleanup tmp files
        for filename in self._files:
            if not 'dask-worker-deployment' in self._files.get(filename):
                path = os.path.join(self._local_workdir, self._files.get(filename) % self._pandaid)
                if os.path.exists(path):
                    base_logger.debug(f'could have removed {path}')
        for worker in range(self._nworkers):
            path = os.path.join(self._local_workdir, self._files.get('dask-worker') % (self._pandaid, worker))
            if os.path.exists(path):
                base_logger.debug(f'could have removed {path}')
