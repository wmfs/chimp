'''
Created on 4 Mar 2012

@author: Tim.Needham
'''
import cs
import chimpsql
import chimpspec

SHARED_SCHEMA="shared"

class ZoneColumn:
    '''
    classdocs
    '''


    def __init__(self, zoneColumnTag):
        self.type = "zoneColumn"
        self.taskOrder = 2
        
        self.outputColumn = cs.grabAttribute(zoneColumnTag,"outputColumn")
        if self.outputColumn is None:
            self.outputColumn = "zone"
        
        self.xColumn = cs.grabAttribute(zoneColumnTag,"xColumn")
        self.yColumn = cs.grabAttribute(zoneColumnTag,"yColumn")
        
        self.triggeringColumns = [self.xColumn, self.yColumn]
    
    def debug(self, appLogger):
        appLogger.debug("    zoneColumn")
        appLogger.debug("      outputColumn : {0}".format(self.outputColumn))
        appLogger.debug("      xColumn      : {0}".format(self.xColumn))
        appLogger.debug("      yColumn      : {0}".format(self.yColumn))

    def getExtraSystemFields(self):
        extraSystemFields = []
        field = chimpspec.SpecificationRecordField(None, None, column=self.outputColumn, type="number", size=3, mandatory=True)
        extraSystemFields.append(field)        
        return(extraSystemFields)

    def requiresFile(self):
        return(False)  
    
    def getTriggeringColumns(self):
        return(self.triggeringColumns)                           
            
    def getComputedZoneFunction(self, sourceName, schemaName, zones, srid, defaultZoneId):
        self.name = "computed_{0}_zone".format(sourceName)
        
        dml = ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
               "  RETURNS trigger AS\n"
               "$BODY$\n"
               "BEGIN\n"
               "  new.{2} = {3}.get_zone(new.{4}, new.{5});\n"                               
               "  RETURN new;\n"
               "END;\n"
               "$BODY$\n"
               "LANGUAGE plpgsql;\n\n".format(schemaName, self.name, self.outputColumn, SHARED_SCHEMA, self.xColumn, self.yColumn))
                
        return chimpsql.Function(self.name, schemaName, [], dml)
        
    def getComputedInsertZoneTrigger(self, sourceName, schemaName, tableName, triggerFunction):
            triggerName = "h_computed_{0}_zone_insert".format(sourceName)
            return chimpsql.Trigger(triggerName, tableName, triggerFunction.name, triggerFunction.schema,
                           ("CREATE TRIGGER {0}\n"
                            "BEFORE INSERT ON {1}.{2}\n"
                            "FOR EACH ROW\n"
                            "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, schemaName, tableName, triggerFunction.schema, triggerFunction.name))
        
    def getComputedUpdateZoneTrigger(self, sourceName, schemaName, tableName, triggerFunction):
            triggerName = "h_computed_{0}_zone_update".format(sourceName)
            return chimpsql.Trigger(triggerName, tableName, triggerFunction.name, triggerFunction.schema,
                           ("CREATE TRIGGER {0}\n"
                            "BEFORE UPDATE OF {1},{2}\n"
                            "ON {3}.{4}\n"
                            "FOR EACH ROW\n"
                            "WHEN (old.{1} IS DISTINCT FROM new.{1} OR old.{2} IS DISTINCT FROM new.{2})\n"
                            "EXECUTE PROCEDURE {5}.{6}();\n\n").format(triggerName, self.xColumn, self.yColumn, schemaName, tableName, triggerFunction.schema, triggerFunction.name))

   