import imp
import os

'''
Created on 26 May 2012

@author: Tim.Needham
'''
def runTool(settings):
    appLogger = settings.appLogger
    appLogger.info("")
    appLogger.info("RUN TOOL")
    appLogger.info("--------")
    
    name = settings.args.name
    profile = settings.args.profile
    extraArgs = settings.extraArgs
    
    appLogger.info("   name        : {0}".format(name))
    appLogger.info("   profile     : {0}".format(profile))
                    
    filename = "{0}.py".format(name)
    fullPath = os.path.join(settings.paths["config"], "tools", name, filename)
    appLogger.debug("  fullPath    : {0}".format(fullPath))
            
    mainModule = imp.load_source(filename, fullPath).Tool
    mainModule(settings, profile, extraArgs, appLogger)
        
