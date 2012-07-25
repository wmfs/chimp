import cs
from operator import attrgetter
from calc.CustomColumn import CustomColumn
from calc.MappedColumn import MappedColumn
from calc.Ctree import Ctree
from calc.Pin import Pin
from calc.SearchEntry import SearchEntry
from calc.TimestampColumn import TimestampColumn
from  calc.SolrDocument import SolrDocument as Doc 
from calc.ZoneColumn import ZoneColumn
import xml.dom.minidom as minidom
import calc.solr as solr

class CalculatedData:
    
    def requiresFile(self):
        result = False
        for element in self.elements:
            if element.requiresFile():
                result = True
        return(result)
        
    
    def writeProcessorFile(self, sourceType, sourceSchema, sourceName, source, file):
                
        content= ("'''\n"
                  "Calculated data processor\n"
                  "-------------------------\n\n"
                  "{0}: {1}.{2}\n\n"
                  "Input columns:\n".format(sourceType, sourceSchema, sourceName))

        
        if sourceType=="table":
            content += "  id bigint NOT NULL\n"
            for column in source.getAllMappedFields():
                content += "{0}\n".format(column.columnClause(None))
                
        elif sourceType=="view":
            for entityTable in source.tables:
                content +="* {0}_id\n".format(entityTable.name)
                for column in entityTable.columns:
                    content +="  {0}\n".format(column.finalEntityColumn)


        content += ("'''\n\n"
                   "class DataCalculator():\n\n")
        
        for element in self.elements:
            if element.requiresFile():
                content += element.getFunctionScript(source)

        content = content.replace("\t","    ")
        file.write(content)





                 
#        if sourceType=="table":
#            inputColumns = getTableInputColumns()
#
#
#        return Table(record.table, schemaName, 
#                     ("CREATE TABLE {0}.{1}(\n"
#                        "  id bigint PRIMARY KEY{2}{3},\n"
#                        "  created timestamp with time zone NOT NULL DEFAULT now(),\n"                        
#                        "  modified timestamp with time zone NOT NULL DEFAULT now(){4}{5}\n"
#                        "){6};\n\n").format(schemaName, record.table,
#                                            "" if schemaName != "editable" else self._getSystemColumnDefsSQL(EDITABLE_SYSTEM_FIELDS),
#                                            self._getColumnDefsSQL(record.getAllMappedFields()),
#                                            "" if schemaName != "import" else self._getSystemColumnDefsSQL(IMPORT_SYSTEM_FIELDS),
#                                            "" if record.getDestinationTargetSchema() != schemaName else self._getSystemColumnDefsSQL(self._getFinalDestinationSystemFields(record)),
#                                            self._getWithOIDSSQL(record)))
        
        
        
        
    
    def debug(self, appLogger):
        if len(self.elements)>0:
            appLogger.debug("")
            appLogger.debug("  Computed data elements... [{0}]".format(self.hostObjectType))
            for element in self.elements:
                element.debug(appLogger)
                appLogger.debug("      triggeringColumns : {0}".format(element.triggeringColumns))
                appLogger.debug("")

    def getAllFields(self):
        fields = []
        for element in self.elements:
            fields.extend(element.getExtraSystemFields())
        return(fields)
#    def addTasks(self, settings, queuer, groupId, stream):
#        for element in sorted(self.elements, key=attrgetter("taskOrder")):
#            element.addTasks(settings, queuer, groupId, stream)

    def getDefaultFillFactor(self):
        
        # Hmmm... still tuning this.
        # 90 for a simple label is too high - it works with 40.
        # Needs to consider the size of the row and the percentage
        # of the rows affected.
        # Defaulting to 60 and see how we go then.
        
        default = None
        
        score = 0
        for element in self.elements:
            if element.type == "ctree":
                score+=2
            if element.type == "customColumn":
                score+=1
        if score==0:
            default = None
        elif score==1:
            default = 60
        elif score==2:
            default = 60
        elif score==3:
            default = 60
        else:
            default = 60
            
        return(default)
        
    def __init__(self, hostObjectType, computedDataTag, settings):        
        solrSettings=None
        solrFields = None
        self.elements=[]
    
        if len(computedDataTag)>0:
            # Grab any triggering columns
            computedDataTag = computedDataTag[0]
            self.hostObjectType = hostObjectType                         
            #computedDataElements = computedDataTag.getElementsByTagName("computedDataElement")
            for computedDataElement in computedDataTag.childNodes:                
                if computedDataElement.localName == "computedDataElement":
                    
                    elements = computedDataElement.childNodes
                    for element in elements:
                        if element.localName=="customColumn":
                            self.elements.append(CustomColumn(element))

                        if element.localName=="mappedColumn":        
                            self.elements.append(MappedColumn(element))
        
                        if element.localName=="ctree":
                            self.elements.append(Ctree(element))
        
                        if element.localName=="pin":
                            self.elements.append(Pin(element, settings))

                        if element.localName== "searchEntry":
                            self.elements.append(SearchEntry(element))
        
                        if element.localName=="timestampColumn":
                            self.elements.append(TimestampColumn(element))
        
                        if element.localName == "zoneColumn":
                            self.elements.append(ZoneColumn(element))
                            
                        if element.localName == "solrDocument":
                            if solrSettings is None:
                                solrSettings = solr.SolrSettings(settings)
                                solrSettings.debug(settings.appLogger)
                                solrFields = solr.SolrFields(settings)
                                solrFields.debug(settings.appLogger)
                            self.elements.append(Doc(element, solrSettings, solrFields, settings))   