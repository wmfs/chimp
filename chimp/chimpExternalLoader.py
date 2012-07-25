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
        
        
    def __init__(self,name,context,configRoot):

        self.name = name
        self.context = context
        self.loaderFile = os.path.join(configRoot,"external loaders", name, "profiles", context,"%s.xml"%(context))

        xmldoc = xml.dom.minidom.parse(self.loaderFile)
        commandConfigTag = xmldoc.getElementsByTagName("commandConfig")
        
        if len(commandConfigTag) >0:
            commandConfigTag = commandConfigTag[0]
    
            self.commandName=cs.grabAttribute(commandConfigTag,"commandName")
            self.currentWorkingDirectory=cs.grabAttribute(commandConfigTag,"currentWorkingDirectory")
                
            self.args=[]
            argumentsTag = commandConfigTag.getElementsByTagName("arguments")

            if len(argumentsTag) >0:
                argumentsTag = argumentsTag[0]
                argTags = argumentsTag.getElementsByTagName("arg")
                
                if len(argTags) >0:
                    for thisArg in argTags:
                        arg = Arg(thisArg)
                        self.args.append(arg)
                
                
                        
                        
                        

#ogr2ogr -f "PostgreSQL" PG:"host=localhost user=postgres dbname=gis password=postgres active_schema=stage" "D:\raw_data\OrdnanceSurvey\Mastermap\FULL\2010-04-27\65728-SJ2434-5i610.gz" -preserve_fid  -t_srs EPSG:27700 -s_srs EPSG:27700 -append
