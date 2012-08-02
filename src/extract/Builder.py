# pl

FILENAME_PREFIX = "extract_"

import os.path
import extract

class Builder:
    
    
    def generateExtractorScript(self, format):

        def writeBasicInit():
            file.write("\tdef __init__(self, dbCursor, recordRestriction, recordParams, startTimestamp, endTimestamp, limit):\n")
            file.write("\t\tself.dbCursor = dbCursor\n")
            file.write("\t\tself.recordRestriction = recordRestriction\n")
            file.write("\t\tself.recordParams = recordParams\n")
            file.write("\t\tself.startTimestamp = startTimestamp\n")
            file.write("\t\tself.endTimestamp = endTimestamp\n")
            file.write("\t\tself.limit = limit\n")
        
        def writeAdditionalCsvInit():
            file.write("\n\t\t#Calculate full filename for the output file\n")
            file.write("\t\tf=()\n\n")
            
        def writeHeader():
            file.write("# Extractor script\n")
            file.write("# ----------------\n")
            file.write("#   Specification  : {0}\n".format(self.specificationName))
            file.write("#   Extract format : {0}\n".format(format))
            file.write("#   Extractor type : {0}\n\n".format(ext.extractor.type))                        
            file.write("class Extractor:\n\n")
        
        def writeGetFilename():
            file.write("\tdef getFilename():")
        
            
            
        def writeCsvExtractor():
            writeAdditionalCsvInit()
            writeGetFilename()
            

        appLogger = self.settings.appLogger
        ext = extract.Extract(self.settings, format)
        ext.debug(appLogger) 
        outputFilename =  os.path.join(self.settings.paths["repository"], "scripts", "generated", "specification files", self.specificationName, "py", "extracts", "{0}_extractor.py".format(format))        
        file = open(outputFilename, "w")
        
        writeHeader()
        writeBasicInit()
        
        if ext.extractor.type=="csvExtractor":
            writeCsvExtractor()
        
        file.close(); 
    
    def generateAllExtractorScripts(self):
        for format in self.formats:
            self.generateExtractorScript(format)
    
    def debug(self, appLogger):
        appLogger.debug("")
        appLogger.debug("Extract Builder")
        appLogger.debug("---------------")
        appLogger.debug("")
        appLogger.debug("formats  : {0}".format(self.formats))
    
    def __init__(self, settings):
        self.formats=[]
        self.settings = settings
        self.specificationName = settings.specification.name
        formatPath =  os.path.join(settings.paths["repository"], "specifications", self.specificationName, "extract_formats")

        for file in os.listdir(formatPath):    
            if file.endswith(".xml"):
                f = os.path.splitext(file)[0]
                f = f[len(FILENAME_PREFIX):]
                self.formats.append(f)
        
        