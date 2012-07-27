'''
Created on 9 Dec 2011

@author: Ryan Pickett
'''

import os
import shutil
from shutil import ignore_patterns


def createRepository(settings):
    appLogger = settings.appLogger
    destPath = settings.env["repositoryPath"]
    appLogger.debug("")
    appLogger.debug("Create repository...")
    appLogger.debug("  destPath   : {0}".format(destPath))

    pathExists = os.path.exists(destPath)
    appLogger.debug("  Root directory exists? : {0}".format(pathExists))
    
    if not pathExists:
        os.makedirs(destPath)
        appLogger.debug("    <made root directory>")

    srcDir = os.path.join(settings.paths["repositoryTemplates"])
    
    dirs = os.listdir(srcDir)
    for d in dirs:
        fullSourcePath = os.path.join(srcDir, d)
        if os.path.isdir(fullSourcePath):
            fullDestPath =  os.path.join(destPath, d)
            shutil.copytree(fullSourcePath, fullDestPath)
            appLogger.debug("  copytree: {0}".format(fullDestPath))
        else:
            shutil.copy(fullSourcePath, destPath)
            appLogger.debug("  copy: {0}".format(fullSourcePath))
          
    

def createSpecification(settings, name):
    
    # In the repository...
    srcDir = os.path.join(settings.paths["repositoryTemplates"], "specification")    
    for dirPart in ["custom", "generated"]:    
        destDir = os.path.join(settings.paths["repository"], "scripts", dirPart, "specification files", name)    
        shutil.copytree(srcDir, destDir, ignore=ignore_patterns(".*"))



    srcDir = os.path.join(settings.paths["specificationTemplates"])  
    destDir = os.path.join(settings.paths["repository"], "specifications", name)    
    shutil.copytree(srcDir, destDir, ignore=ignore_patterns(".*"))
    

    srcFile = os.path.join(settings.paths["repositoryTemplates"], "specification.xml")
    destFile = os.path.join(settings.paths["repository"], "specifications", name, "{0}.xml".format(name))
    shutil.copy(srcFile, destFile)
    
def createSolrServer(settings, name):
    srcDir = os.path.join(settings.paths["repositoryTemplates"], "solr server")
    
    for dirPart in ["custom", "generated"]:    
        destDir = os.path.join(settings.paths["repository"], "scripts", dirPart, "solr server files", name)    
        shutil.copytree(srcDir, destDir, ignore=ignore_patterns(".*"))

    srcFile = os.path.join(settings.paths["repositoryTemplates"], "solr server.xml")
    destFile = os.path.join(settings.paths["repository"], "solr servers", name + ".xml")

    shutil.copy(srcFile, destFile)    