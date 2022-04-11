#import os
#import argparse
#import traceback
#from urllib.parse import unquote
#from concurrent.futures import ThreadPoolExecutor

from pandaharvester.harvestercore import core_utils
from pandaharvester.harvestercore.plugin_base import PluginBase
#from pandaharvester.harvestermisc.k8s_utils import k8s_Client
#from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestermisc.info_utils import PandaQueuesDict
#from pandaharvester.harvestercore.queue_config_mapper import QueueConfigMapper
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
        # self.dask_client = dask_Client(namespace=namespace, queue_name=self.queueName)
