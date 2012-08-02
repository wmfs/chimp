'''
Created on 4 Mar 2012

@author: Tim.Needham
'''
import cs
import chimpsql
import chimpspec
class TimestampColumn:
    '''
    classdocs
    '''


    def __init__(self, timestampColumnTag):
        self.type = "timestampColumn"
        self.taskOrder = 2
        self.outputColumn = cs.grabAttribute(timestampColumnTag,"outputColumn")
        
        self.triggeringColumns=[]
        triggeringColumnsTag = timestampColumnTag.getElementsByTagName("triggeringColumns")        
        if len(triggeringColumnsTag)>0:
            for column in triggeringColumnsTag[0].getElementsByTagName("column"):
                columnName = cs.grabAttribute(column, "name")
                self.triggeringColumns.append(columnName)        
    
    def debug(self, appLogger):
        appLogger.debug("    timestampColumn")
        appLogger.debug("      outputColumn   : {0}".format(self.outputColumn))

    def getExtraSystemFields(self):
        extraSystemFields = []
        field = chimpspec.SpecificationRecordField(None, None, column=self.outputColumn, type="datetime", mandatory=True, default="now()")
        extraSystemFields.append(field)        
        return(extraSystemFields)     

    def requiresFile(self):
        return(False)    

    def getTriggeringColumns(self):
        return(self.triggeringColumns)                           
        

    def getComputedTimestampFunction(self, sourceName, schemaName):
        self.name = "computed_{0}_{1}_timestamp_update".format(sourceName, self.outputColumn)
        
        dml = ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
               "  RETURNS trigger AS\n"
               "$BODY$\n"
               "  BEGIN\n"
               "    new.{2} = now();\n"                               
               "    RETURN new;\n"
               "  END;\n"
               "$BODY$\n"
               "LANGUAGE plpgsql;\n\n".format(schemaName, self.name, self.outputColumn))
                
        return chimpsql.Function(self.name, schemaName, [], dml)
    
    def getComputedTimestampTrigger(self, sourceName, schemaName, tableName, triggerFunction):
        triggerName = "h_computed_{0}_{1}_timestamp_update".format(sourceName, self.outputColumn)
        when = " OR ".join(map(lambda column: "old.{0} IS DISTINCT FROM new.{0}".format(column), self.triggeringColumns))
        return chimpsql.Trigger(triggerName, tableName, triggerFunction.name, triggerFunction.schema,
                       ("CREATE TRIGGER {0}\n"
                        "BEFORE UPDATE OF {1}\n"
                        "ON {2}.{3}\n"
                        "FOR EACH ROW\n"
                        "WHEN ({4})\n"
                        "EXECUTE PROCEDURE {5}.{6}();\n\n").format(triggerName, ", ".join(self.triggeringColumns), schemaName, tableName, when, schemaName, triggerFunction.name))

    def getComputedTimestampIndex(self, sourceName, schemaName, storageTableName):
        indexName = "{0}_{1}_{2}_timestamp".format(schemaName, sourceName, self.outputColumn)
        return chimpsql.Index(indexName, storageTableName, schemaName,
                     "CREATE INDEX {0} ON {1}.{2} ({3});\n".format(indexName, schemaName, storageTableName, self.outputColumn))   

 