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
    noExamples = settings.args.noexamples
    appLogger.debug("")
    appLogger.debug("Create repository...")
    appLogger.debug("  destPath   : {0}".format(destPath))
    appLogger.debug("  noExamples : {0}".format(noExamples))

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
        
        if not noExamples:
            createSpecification(settings, "example")
            createSolrServer(settings, "default")
    else:
        appLogger.error("Repository already exists!")
        print("Repository already exists!")
    

def createSpecification(settings, name):
    appLogger = settings.appLogger
    rootPath = os.path.join(settings.env["repositoryPath"],"specifications", name)
    appLogger.debug("")
    appLogger.debug("Create specification...")
    appLogger.debug("  rootPath   : {0}".format(rootPath))

    pathExists = os.path.exists(rootPath)
    appLogger.debug("  Root directory exists? : {0}".format(pathExists))
    
    if not pathExists:
        os.makedirs(rootPath)
        appLogger.debug("    <made root directory>")

        srcDir = os.path.join(settings.paths["specificationTemplate"])
        
        dirs = os.listdir(srcDir)
        for d in dirs:
            fullSourcePath = os.path.join(srcDir, d)
            if os.path.isdir(fullSourcePath):
                fullDestPath =  os.path.join(rootPath, d)
                shutil.copytree(fullSourcePath, fullDestPath)
                appLogger.debug("  copytree: {0}".format(fullDestPath))
            else:
                shutil.copy(fullSourcePath, rootPath)
                appLogger.debug("  copy: {0}".format(fullSourcePath))
    
    else:
        appLogger.error("Specification already exists!")
        print("Specification already exists!")

    
def createSolrServer(settings, name):
    appLogger = settings.appLogger
    rootPath = os.path.join(settings.env["repositoryPath"],"solr_servers", name)
    appLogger.debug("")
    appLogger.debug("Create Solr server...")
    appLogger.debug("  destPath   : {0}".format(rootPath))

    pathExists = os.path.exists(rootPath)
    appLogger.debug("  Root directory exists? : {0}".format(pathExists))
    
    if not pathExists:
        os.makedirs(rootPath)
        appLogger.debug("    <made root directory>")

        srcDir = os.path.join(settings.paths["solrServerTemplate"])
        
        dirs = os.listdir(srcDir)
        for d in dirs:
            fullSourcePath = os.path.join(srcDir, d)
            if os.path.isdir(fullSourcePath):
                fullDestPath =  os.path.join(rootPath, d)
                shutil.copytree(fullSourcePath, fullDestPath)
                appLogger.debug("  copytree: {0}".format(fullDestPath))
            else:
                shutil.copy(fullSourcePath, rootPath)
                appLogger.debug("  copy: {0}".format(fullSourcePath))


    else:
        appLogger.error("Solr server already exists!")
        print("Solr server already exists!")


#    srcDir = os.path.join(settings.paths["repositoryTemplates"], "solr server")
#    
#    for dirPart in ["custom", "generated"]:    
#        destDir = os.path.join(settings.paths["repository"], "scripts", dirPart, "solr server files", name)    
#        shutil.copytree(srcDir, destDir, ignore=ignore_patterns(".*"))
#
#    srcFile = os.path.join(settings.paths["repositoryTemplates"], "solr server.xml")
#    destFile = os.path.join(settings.paths["repository"], "solr servers", name + ".xml")
#
#    shutil.copy(srcFile, destFile)    