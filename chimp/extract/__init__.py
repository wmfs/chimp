import xml.dom.minidom
import cs
import os.path


#        <destination encoding="Hmmmm" defaultTargetDirectory="d:\">
#            <filename format="abp_nlpg_extract_{0}.csv">
#                <input variable="extractionStartDatetime" />
#            </filename>        
#        </destination>
#                
#        <formatting delimiter="," qualifier='"' dateFormat="%Y-%m-%d"
#            timeFormat="%H%M%S" qualifiedDates="false"
#            qualifiedTimes="false"/>
#            
#            

class OutputField():   
    def debug(self, index, appLogger):
        appLogger.debug('    [{0}] - {1} (sourceType="{2}" sourceValue="{3}" outputType="{4}" outputDecimalPlaces="{5}" optionSetName="{6}" optionSetColumn="{7}" size="{8}" valueIfUnmapped="{9}")'.format(index, self.name, self.sourceType, self.sourceValue, self.outputType, self.outputDecimalPlaces, self.optionSetName, self.optionSetColumn, self.size, self.valueIfUnmapped))
  
    
    def __init__(self, tag):
        self.name = cs.grabAttribute(tag, "name")
        self.sourceType = cs.grabAttribute(tag, "sourceType")
        self.sourceValue = cs.grabAttribute(tag, "sourceValue")
        self.outputType = cs.grabAttribute(tag, "outputType")
        self.outputDecimalPlaces = cs.grabAttribute(tag, "outputDecimalPlaces")
        self.optionSetName = cs.grabAttribute(tag, "optionSetName")
        self.optionSetColumn = cs.grabAttribute(tag, "optionSetColumn")
        self.size = cs.grabAttribute(tag, "size")
        self.valueIfUnmapped = cs.grabAttribute(tag, "valueIfUnmapped")


class Line():
    def debug(self, appLogger):
        i=0
        for outputField in self.outputFields:
            outputField.debug(i, appLogger)
            i += 1

    def __init__(self, tag):
        self.outputFields = []
        outputFieldsTag = tag.getElementsByTagName("outputFields")
        if len(outputFieldsTag)==1:
            outputFieldsTag = outputFieldsTag[0]
            outputFieldTags = tag.getElementsByTagName("outputField")
            for outputFieldTag in outputFieldTags:
                self.outputFields.append(OutputField(outputFieldTag))

class Table():
    
    def debug(self, appLogger):
        appLogger.debug("      {0}.{1}: (timestampColumn={2} alias={3})".format(self.schema, self.name, self.timestampColumn, self.alias))
        appLogger.debug("        {0}".format(self.keyColumns))
        
    
    def __init__(self, tag):
        self.schema = cs.grabAttribute(tag, "schema")
        self.name = cs.grabAttribute(tag, "name")
        self.timestampColumn = cs.grabAttribute(tag, "timestampColumn")
        self.alias = cs.grabAttribute(tag, "alias")        
        keyTag = tag.getElementsByTagName("key")
        if len(keyTag)==1:
            keyTag = keyTag[0]
            columnTags = keyTag.getElementsByTagName("column")
            self.keyColumns = list(map(lambda c:cs.grabAttribute(c, "name"), columnTags))
        else:
            self.keyColumns = []
            

class Source():
    
    def debug(self, appLogger):
        if self.defined:
            for table in self.tables:
                table.debug(appLogger)
        else:
            appLogger.debug("      Not defined")
    
    def __init__(self, tag):
        if tag is not None:
            if len(tag)==1:
                tag = tag[0]
                self.defined = True
                self.tables = []
                tableTags = tag.getElementsByTagName("table")
                if len(tableTags) > 0:
                    for tableTag in tableTags:
                        self.tables.append(Table(tableTag))  
            else:
                self.defined = False
        else:
            self.defined = False
  


class Section():
    
    def debug(self, appLogger):
        appLogger.debug("* {0}".format(self.name))
        appLogger.debug("  Sources:")
        appLogger.debug("    fullSource:")
        self.fullSource.debug(appLogger)
        appLogger.debug("    changeSource:")
        self.changeSource.debug(appLogger)
        appLogger.debug("    deleteSource:")
        self.deleteSource.debug(appLogger)
        appLogger.debug("  Line:")
        for line in self.lines:
            line.debug(appLogger)
        
    
    def __init__(self, tag):
        self.name = cs.grabAttribute(tag , "name")
        sourceTag = tag.getElementsByTagName("source")
        self.lines=[]
        
        if len(sourceTag) ==1:  
            sourceTag = sourceTag[0]      
            self.fullSource = Source(sourceTag.getElementsByTagName("fullPrimaryKeys"))
            self.changeSource = Source(sourceTag.getElementsByTagName("changePrimaryKeys"))
            self.deleteSource = Source(sourceTag.getElementsByTagName("deletePrimaryKeys"))
            
            lineTags = tag.getElementsByTagName("line")
            for line in lineTags:
                self.lines.append(Line(line))
        else:
            self.fullSource = Source(None)
            self.changeSource = Source(None)
            self.deleteSource = Source(None)
            
class CsvExtractor:
    def debug(self, appLogger):
        appLogger.debug("  encoding               : {0}".format(self.encoding))
        appLogger.debug("  defaultTargetDirectory : {0}".format(self.defaultTargetDirectory))
        appLogger.debug("  filenameFormat         : {0}".format(self.filenameFormat))
        appLogger.debug("  filenameInputVariables : {0}".format(self.filenameInputVariables))
        appLogger.debug("  delimiter          : {0}".format(self.delimiter))
        appLogger.debug("  qualifier          : {0}".format(self.qualifier))
        appLogger.debug("  dateFormat         : {0}".format(self.dateFormat))
        appLogger.debug("  timeFormat         : {0}".format(self.timeFormat))
        appLogger.debug("  dateTimeFormat     : {0}".format(self.dateTimeFormat))
        appLogger.debug("  dateTimeFormat     : {0}".format(self.dateTimeFormat))
        appLogger.debug("  qualifiedDates     : {0}".format(self.qualifiedDates))
        appLogger.debug("  qualifiedDateTimes : {0}".format(self.qualifiedDateTimes))
        appLogger.debug("  qualifiedTimes     : {0}".format(self.qualifiedTimes))
        appLogger.debug("")
        appLogger.debug("Sections")
        appLogger.debug("--------")        
        for section in self.sections:
            section.debug(appLogger)
            appLogger.debug("")
         
    def __init__(self, tag):
        self.type = "csvExtractor"
        
        destinationTag = tag.getElementsByTagName("destination")        
        if destinationTag is not None:
            destinationTag = destinationTag[0]
            self.encoding = cs.grabAttribute(destinationTag , "encoding")
            self.defaultTargetDirectory = cs.grabAttribute(destinationTag, "defaultTargetDirectory")                
            filenameTag = destinationTag.getElementsByTagName("filename")
            if filenameTag is not None:
                filenameTag = filenameTag[0]
                self.filenameFormat = cs.grabAttribute(filenameTag , "format")
                self.filenameInputVariables = []
                inputTags = filenameTag.getElementsByTagName("input")
                for input in inputTags:
                    self.filenameInputVariables.append(cs.grabAttribute(input , "variable"))    

        formattingTag = tag.getElementsByTagName("formatting")        
        if formattingTag is not None:
            formattingTag = formattingTag[0]
            self.delimiter = cs.grabAttribute(formattingTag , "delimiter")
            self.qualifier = cs.grabAttribute(formattingTag , "qualifier")
            self.dateFormat = cs.grabAttribute(formattingTag , "dateFormat")
            self.timeFormat = cs.grabAttribute(formattingTag , "timeFormat")
            self.dateTimeFormat = cs.grabAttribute(formattingTag , "dateTimeFormat")
            self.dateTimeFormat = cs.grabAttribute(formattingTag , "dateTimeFormat")
            
            self.qualifiedDates = cs.grabAttribute(formattingTag , "qualifiedDates")
            if self.qualifiedDates is not None:
                if self.qualifiedDates.lower()=="true":
                    self.qualifiedDates = True
                else:
                    self.qualifiedDates = False
            else:
                self.qualifiedDates = False
                
                
            self.qualifiedDateTimes = cs.grabAttribute(formattingTag , "qualifiedDateTimes")
            if self.qualifiedDateTimes is not None:
                if self.qualifiedDateTimes.lower()=="true":
                    self.qualifiedDateTimes = True
                else:
                    self.qualifiedDateTimes = False
            else:
                self.qualifiedDateTimes = False

            self.qualifiedTimes = cs.grabAttribute(formattingTag , "qualifiedTimes")
            if self.qualifiedTimes is not None:
                if self.qualifiedTimes.lower()=="true":
                    self.qualifiedTimes = True
                else:
                    self.qualifiedTimes = False
            else:
                self.qualifiedTimes = False
        
        self.sections=[]
        sectionsTag = tag.getElementsByTagName("sections")
        if sectionsTag is not None:
            sectionsTag = sectionsTag[0]
            sectionTags = sectionsTag.getElementsByTagName("section")
            
            for sectionTag in sectionTags:
                self.sections.append(Section(sectionTag))


        
class Extract:
    
    def debug(self, appLogger):
        appLogger.debug("")
        appLogger.debug("Extract")
        appLogger.debug("-------")
        appLogger.debug("specificationName  :  {0}".format(self.specificationName))
        appLogger.debug("format             :  {0}".format(self.format))
        appLogger.debug("extractXmlFilename :  {0}".format(self.extractXmlFilename))
        appLogger.debug("Constants:")
        appLogger.debug("  {0}".format(self.constants))
        appLogger.debug("")
        appLogger.debug("Extractor:")
        appLogger.debug("  type               : {0}".format(self.extractor.type))        
        self.extractor.debug(appLogger)
            
    def __init__(self, settings, format=None):
        
        
        self.specification = settings.specification
        self.specificationName = self.specification.name
        
        if format is None:
            self.format = settings.args.format
        else:
            self.format = format
        
        self.extractXmlFilename = os.path.join(settings.paths["repository"],"specifications", self.specificationName, "extract formats", "extract_{0}.xml".format(self.format))  
        xmldoc = xml.dom.minidom.parse(self.extractXmlFilename)
        extractTag = xmldoc.getElementsByTagName("extract")[0]
        
        self.constants = {}
        constantsTag = extractTag.getElementsByTagName("constants")
        if constantsTag is not None:
            constantsTag = constantsTag[0]
            constantTags = xmldoc.getElementsByTagName("constant")
            for constantTag in constantTags:
                name = cs.grabAttribute(constantTag , "name") 
                value = cs.grabAttribute(constantTag , "value")
                self.constants[name]=value
                
        
        csvExtractorTag = extractTag.getElementsByTagName("csvExtractor")
        if len(csvExtractorTag)==1:
            self.extractor =  CsvExtractor(csvExtractorTag[0])

        xmldoc.unlink()                