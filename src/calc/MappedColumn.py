'''
Created on 4 Mar 2012

@author: Tim.Needham
'''

import cs
import chimpspec

class MappedColumn:
    '''
    classdocs
    '''
    
    def __init__(self, mappedColumnTag):
        self.type = "mappedColumn"
        self.taskOrder = 3
        self.outputColumn = cs.grabAttribute(mappedColumnTag,"outputColumn")

        self.field = chimpspec.SpecificationRecordField(mappedColumnTag, None)
                
        self.inputColumn = cs.grabAttribute(mappedColumnTag,"inputColumn")
        self.optionSetName = cs.grabAttribute(mappedColumnTag,"optionSetName")
        self.optionSetColumn = cs.grabAttribute(mappedColumnTag,"optionSetColumn")
        if self.optionSetColumn is None:
            self.optionSetColumn = "label"
            
        self.valueIfUnmapped = cs.grabAttribute(mappedColumnTag,"valueIfUnmapped")
    
        self.triggeringColumns = [self.inputColumn]
        
    def debug(self, appLogger):
        appLogger.debug("    mappedColumn")
        appLogger.debug("      outputColumn   : {0}".format(self.outputColumn))
        appLogger.debug("      type           : {0}".format(self.field.type))
        appLogger.debug("      size           : {0}".format(self.field.size))
        appLogger.debug("      decimalPlaces  : {0}".format(self.field.decimalPlaces))
        appLogger.debug("      inputColumn    : {0}".format(self.inputColumn))
        appLogger.debug("      optionSetName  : {0}".format(self.optionSetName))
        appLogger.debug("      optionSetColumn: {0}".format(self.optionSetColumn))
        appLogger.debug("      valueIfUnmapped: {0}".format(self.valueIfUnmapped))

    def getExtraSystemFields(self):
        extraSystemFields = []
        extraSystemFields.append(self.field)
        return(extraSystemFields)
    
    def requiresFile(self):
        return(False)  
    

    def getTriggeringColumns(self):
        return([])                           
                
    def getPinScript(self):
        
        body = "\n\t\t#Lookup '{0}' from 'reference.{1}'\n".format(self.optionSetColumn, self.optionSetName)
        body += '\t\tsql="select {0} from reference.{1} where value=%s"\n'.format(self.optionSetColumn, self.optionSetName)
        body += '\t\tdbCursor.execute(sql, (data["{0}"],))\n'.format(self.inputColumn)
        body += "\t\tresult = dbCursor.fetchone()\n".format(self.inputColumn)
        body += "\t\tif result is not None:\n"
        body += "\t\t\tr.append(result[0]) # --> {0}\n".format(self.outputColumn)
        body += "\t\telse:\n"
        
        if self.valueIfUnmapped is not None:
            if self.field.columnDataType in("integer","bigint"):
                body += "\t\t\tr.append(int({0})\n".format(self.valueIfUnmapped)
            elif self.field.columnDataType in("decimal","double precision"):
                body += "\t\t\tr.append(float({0})\n".format(self.valueIfUnmapped)
            elif self.field.columnDataType=="character varying":
                body += '\t\t\tr.append("{0}")\n'.format(self.valueIfUnmapped)
            else:
                body += '\t\t\tr.append({0})\n'.format(self.valueIfUnmapped)
        else:
            body += '\t\t\tr.append(None)\n'       
        return(body)




        #sql = "select last_sent_to_editable from shared.specification_registry where name=%s"
        #self.supportCursor.execute(sql, (specification.name,))
        #lastImportTimestamp = self.supportCursor.fetchone()[0]
        