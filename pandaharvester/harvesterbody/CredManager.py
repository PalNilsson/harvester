import datetime
import threading

from pandaharvester.harvesterconfig import harvester_config
from pandaharvester.harvestercore import CoreUtils
from pandaharvester.harvestercore.PluginFactory import PluginFactory


# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('CredManager')


# credential manager
class CredManager (threading.Thread):

    # constructor
    def __init__(self,singleMode=False):
        threading.Thread.__init__(self)
        self.singleMode = singleMode
        self.pluginFactory = PluginFactory()
        # get plugin
        pluginPar = {}
        pluginPar['module'] = harvester_config.credmanager.moduleName
        pluginPar['name']   = harvester_config.credmanager.className
        pluginPar['config'] = harvester_config.credmanager
        self.exeCore = self.pluginFactory.getPlugin(pluginPar)



    # main loop
    def run (self):
        while True:
            # execute
            self.execute()
            # escape if single mode
            if self.singleMode:
                return
            # sleep
            CoreUtils.sleep(harvester_config.credmanager.sleepTime)



    # main
    def execute(self):
        # make logger
        mainLog = CoreUtils.makeLogger(_logger)
        # check credential
        mainLog.debug('check credential')
        isValid = self.exeCore.checkCredential(harvester_config.pandacon.key_file)
        # renew it if nessesary
        if not isValid:
            mainLog.debug('renew credential')
            tmpStat,tmpOut = self.exeCore.renewCredential(harvester_config.pandacon.key_file)
            if not tmpStat:
                mainLog.error('failed : {0}'.format(tmpOut))
                return
        mainLog.debug('done')
