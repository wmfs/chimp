import cs
import os
import xml.dom.minidom
import subprocess
import shlex

def stageUsingExternalLoader(supportConnection, supportCursor, settings, taskId, processLimit, args):
    successCount = 0
    exceptionCount = 0
    errorCount =0 
    warningCount = 0 
    ignoredCount = 0 
    noticeCount = 0
    asTyped='"%s" %s' %(args["commandname"], args["commandargs"])
    print("Trying: %s" %(asTyped))
    splitArgs=shlex.split(asTyped)
    process = subprocess.call(splitArgs ,cwd=args["currentworkingdirectory"])
 #cwd='"%s"' %(args["currentworkingdirectory"])   
    return((successCount,exceptionCount,errorCount,warningCount,ignoredCount,noticeCount))



class Arg():
    def __init__(self, argTag):
        self.value=cs.grabAttribute(argTag,"value")
        self.placeholders=[]
        placeholderContentTag = argTag.getElementsByTagName("placeholderContent")
        if len(placeholderContentTag) >0:
            placeholderContentTag=placeholderContentTag[0]
            placeholderTags = placeholderContentTag.getElementsByTagName("placeholder")
            for thisPlaceholder in placeholderTags:
                value = cs.grabAttribute(thisPlaceholder,"variable")
                self.placeholders.append(value)

class ExternalLoader():
    def getCommandName(self):
        return (self.commandName)
    
    def getCurrentWorkingDirectory(self):
        return (self.currentWorkingDirectory)
    
    def getFullCommand(self, valuePool):
        parts=[] 
        for thisArg in self.args:
            value = thisArg.value
            
            for thisPlaceHolder in thisArg.placeholders:
                value = value.replace("%s",valuePool[thisPlaceHolder],1)
            
            parts.append(value)
        command = cs.delimitedStringList(parts, " ")
        return(command)
        
        
    def __init__(self,name, profile, configRoot):

        self.name = name
        self.profile = profile
        
        # Grab registry settings
        
        self.configFile = os.path.join(configRoot,"external_loaders", name, "loader_config.xml")
        xmldoc = xml.dom.minidom.parse(self.configFile)
        loaderConfigTag = xmldoc.getElementsByTagName("loaderConfig")[0]
        registerTag = loaderConfigTag.getElementsByTagName("registry")[0]
        
        self.registry={}
        keyTags = registerTag.getElementsByTagName("key") 
        for keyTag in keyTags:
            keyName = cs.grabAttribute(keyTag,"name")
            keyValue =cs.grabAttribute(keyTag,"value")
            self.registry[keyName]=keyValue
                
        xmldoc.unlink()
        self.commandName=self.registry["commandName"]
        self.currentWorkingDirectory=self.registry["currentWorkingDirectory"]


        #Grab profile file
        
        self.profileFile = os.path.join(configRoot,"external_loaders", name, "profiles", "{0}.xml".format(profile))

        xmldoc = xml.dom.minidom.parse(self.profileFile)
        externalLoaderProfileTag = xmldoc.getElementsByTagName("externalLoaderProfile")
        
        if len(externalLoaderProfileTag) >0:
            externalLoaderProfileTag = externalLoaderProfileTag[0]
    
            profileCommandName=cs.grabAttribute(externalLoaderProfileTag,"commandName")
            if profileCommandName is not None:
                self.commandName = profileCommandName 
                
            profileCurrentWorkingDirectory=cs.grabAttribute(externalLoaderProfileTag,"currentWorkingDirectory")
            if profileCurrentWorkingDirectory is not None:
                self.currentWorkingDirectory=profileCurrentWorkingDirectory
            
                        
            self.args=[]
            argumentsTag =  externalLoaderProfileTag.getElementsByTagName("arguments")

            if len(argumentsTag) >0:
                argumentsTag = argumentsTag[0]
                argTags = argumentsTag.getElementsByTagName("arg")
                
                if len(argTags) >0:
                    for thisArg in argTags:
                        arg = Arg(thisArg)
                        self.args.append(arg)
                
                
                        
                        
                        

#ogr2ogr -f "PostgreSQL" PG:"host=localhost user=postgres dbname=gis password=postgres active_schema=stage" "D:\raw_data\OrdnanceSurvey\Mastermap\FULL\2010-04-27\65728-SJ2434-5i610.gz" -preserve_fid  -t_srs EPSG:27700 -s_srs EPSG:27700 -append
