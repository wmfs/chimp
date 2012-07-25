import imp
import re
import os
import argparse
import fnmatch
import xml.dom.minidom
import cs
import platform

class Tool:

    def __init__(self, settings, profile, extraParams, appLogger):

        def getProfileSettings(profile):
            profileSettings = {}
            filename = "rename_{0}.xml".format(profile)
            fullPath = os.path.join(settings.paths["config"], "tools", "rename", "profiles", profile, filename)
            xmldoc = xml.dom.minidom.parse(fullPath)
            settingsTag = xmldoc.getElementsByTagName("settings")[0]            
            keyTags = settingsTag.getElementsByTagName("key")
            for key in keyTags:
                name = cs.grabAttribute(key, "name")
                value = cs.grabAttribute(key, "value")
                profileSettings[name] = value
            return(profileSettings)

        def getFiles(root, recurse, extensions):
            l = []
            if recurse:
                for root, dirnames, filenames in os.walk(root):
                    if len(extensions)>0:
                        for e in extensions:
                            for filename in fnmatch.filter(filenames, "*.{0}".format(e)):
                                fullFilename = os.path.join(root,filename)
                                l.append(fullFilename)
    
            else:
                filenames = os.listdir(root)
                if len(extensions)>0:
                    for e in extensions:        
                        for filename in fnmatch.filter(filenames, "*.{0}".format(e)):
                            fullFilename = os.path.join(root,filename)
                            l.append(fullFilename)
            return(l)



        
        # Parse arguments for this tool
        # -----------------------------
        parser = argparse.ArgumentParser(description="A tool within Chimp to rename files")
        parser.add_argument("--recurse", action="store_true", default="normal", help="S")
        parser.add_argument("--case", action="store", default="preserve", choices=("preserve","upper","lower"))
        parser.add_argument("--extensionrestriction", action="store")
        parser.add_argument("--filenameregex", action="store")
        parser.add_argument("--deferprocessing", action="store_true")
        parser.add_argument("files", nargs="*", help="Defines individual files or groups")                
        args = parser.parse_args(extraParams)
        appLogger.info("   args        : {0}".format(args))


        
        # Grab profile and settings   
        # -------------------------     
        if profile is None:
            profile = "default" 
        appLogger.info("   profile     : {0}".format(profile))  
        profileSettings = getProfileSettings(profile)                
                   

        
        # Merge profile settings with those passed as args
        # -------------------------------------------------        
        
        # recurse
        if "recurse" in profileSettings:
            if profileSettings["recurse"]=="true":
                recurse = True
            elif profileSettings["recurse"]=="false":
                recurse = False
        else:
            recurse = args.recurse

        # Case
        if "case" in profileSettings:
            case = profileSettings["case"]
        else:
            case = args.case
            
        # extensionRestriction
        if "extensionRestriction" in profileSettings:
            extensionRestriction = profileSettings["extensionRestriction"]     
        else:
            extensionRestriction = extensionrestriction.args
        
        # filenameRegex    
        if "filenameRegex" in profileSettings:            
            filenameRegex = profileSettings["filenameRegex"]
            compiledFilenameRe = re.compile(filenameRegex)
        else:
            filenameRegex = args.filenameregex
            if filenameRegex is not None:
                compiledFilenameRe = re.compile(filenameRegex)
            else:
                compiledFilenameRe = None
            
        # deferPrcessing
        if "deferProcessing" in profileSettings:
            deferProcessing = profileSettings["deferProcessing"]
        else:
            deferProcessing = args.deferprocessing
         
        
        # What command to use for a rename..?
        auditOperatingSystem=platform.system()
        if auditOperatingSystem=="Windows":
            renameCommand = "move"
            scriptExtension = "bat"
            firstLine = None
        elif auditOperatingSystem=="Linux":
            renameCommand = "mv"
            scriptExtension = "sh"
            firstLine = "#!/bin/sh\n"



        # Start splitting things up
        # -------------------------
        root = args.files[0]  
        baseName = os.path.basename(root)                                      
        if extensionRestriction is not None:
            extensions= extensionRestriction.split(",")
        else:
            extensions=[]
        if "*" in baseName:
            extensions.append(baseName.replace("*","").replace(".",""))
            extensions = list(set(extensions))                
            root = os.path.split(root)[0]

        
        # Logging
        # -------
        appLogger.debug("   recurse              : {0}".format(recurse))                
        appLogger.debug("   case                 : {0}".format(case))
        appLogger.debug("   extensionRestriction : {0}".format(extensionRestriction))
        appLogger.debug("   deferProcessing      : {0}".format(deferProcessing))
        appLogger.debug("   extensions           : {0}".format(extensions))
        appLogger.debug("   root                 : {0}".format(root))
        appLogger.debug("   renameCommand        : {0}".format(renameCommand))
        appLogger.debug("   scriptExtensio       : {0}".format(scriptExtension))

        # Grab formatter function
        # -----------------------
        
        filename = "rename_{0}.py".format(profile)
        fullPath = os.path.join(settings.paths["config"], "tools", "rename", "profiles", profile, filename)
        module = imp.load_source(filename, fullPath)
        filenameFormatter= module.filenameFormatter
        
        # Establish an output file
        # ------------------------
        fullPath = os.path.join(settings.paths["temp"], "rename_script.{0}".format(scriptExtension))
        scriptFile = open(fullPath, "w")
        if firstLine is not None:
            scriptFile.write(firstLine)
              
        
        
        # Now mangle each filename
        # for inputFile in inputFiles:
        # ----------------------------
    
        inputFiles = getFiles(root, recurse, extensions)
        renamedFiles =[]
                            
        for inputFile in inputFiles:            

            # Split everything up
            # -------------------
            path = os.path.split(inputFile)[0]
            f = os.path.split(inputFile)[1]
            f = f.split(".")
            filename = f[0]
            if len(f)==2:
                extension=f[1]
            else:
                extension=None            
                        
            adjusted = filenameFormatter(path, filename, extension, case, compiledFilenameRe)            
            command = '{0} "{1}" "{2}"\n'.format(renameCommand, inputFile, adjusted)
            scriptFile.write(command)
        scriptFile.close()