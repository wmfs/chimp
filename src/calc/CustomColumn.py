'''
Created on 4 Mar 2012

@author: Tim.Needham
'''
CALC_SCHEMA = "calc"

import cs
import chimpspec
import chimpsql
import os
import calc
from calc.ContentAssembler import ContentAssembler





class CustomColumn:

    class ContentElement:
        
        def __init__(self, contentElementTag):
            self.prefix = cs.grabAttribute(contentElementTag,"prefix")
            self.column = cs.grabAttribute(contentElementTag,"column")
            self.suffix = cs.grabAttribute(contentElementTag,"suffix")
            
        def debug(self, appLogger):
            appLogger.debug("          * {0}|{1}|{2}".format(self.prefix, self.column, self.suffix))
            
        

    def __init__(self, customColumnTag):
        
        self.type = "customColumn"
        self.taskOrder = 1
        self.outputColumn = cs.grabAttribute(customColumnTag,"outputColumn")
        
        self.field = chimpspec.SpecificationRecordField(customColumnTag, None)

        assembler = customColumnTag.getElementsByTagName("contentAssembler")
        if len(assembler)>0:                
            self.contentAssembler = ContentAssembler(assembler[0])
        else:
            self.contentAssembler = None

        self.triggeringColumns=[]
        triggeringColumnsTag = customColumnTag.getElementsByTagName("triggeringColumns")        
        if len(triggeringColumnsTag)>0:
            for column in triggeringColumnsTag[0].getElementsByTagName("column"):
                columnName = cs.grabAttribute(column, "name")
                self.triggeringColumns.append(columnName)        
        
        templateAssemblerTag = customColumnTag.getElementsByTagName("templateAssembler")
        self.templateAssembler = ContentAssembler(templateAssemblerTag)
                        
    def getTriggeringColumns(self):
        return(map(lambda field:"{0}".format(field.column), self.templateAssembler.contentElements))                           
                    
                    
    def debug(self, appLogger):
        appLogger.debug("    customColumn")
        appLogger.debug("      outputColumn : {0}".format(self.outputColumn))        
        appLogger.debug("      label        : {0}".format(self.field.label))
        appLogger.debug("      type         : {0}".format(self.field.type))
        appLogger.debug("      size         : {0}".format(self.field.size))
        appLogger.debug("      array        : {0}".format(self.field.array))
        appLogger.debug("      decimalPlaces: {0}".format(self.field.decimalPlaces))
        
        self.templateAssembler.debug(appLogger)
        
    def getExtraSystemFields(self):
        extraSystemFields = []
        extraSystemFields.append(self.field)        
        return(extraSystemFields)
    
    def requiresFile(self):
        return(True)


    def getCustomRegistrationDML(self, specificationName, schemaName, sourceName, seq, repositoryPath):

        # 1: specification_name,
        # 2: source_schema,
        # 3: source_name,
        # 4: output_column_list,
        # 5: seq,
        # 6: input_id_column,
        # 7: input_column_list,
        # 8: processing_script_location
        
        inputColumnList = "1???"
        return chimpsql.DML(("SELECT {0}.register_custom('{1}', '{2}', '{3}', '{4}', "
                    "{5}, '{6}');\n\n").format(CALC_SCHEMA,  
                                                 specificationName, 
                                                 schemaName, 
                                                 sourceName,
                                                 self.outputColumn,
                                                 seq,
                                                 inputColumnList),
                    dropDdl="SELECT {0}.unregister_custom('{1}','{2}','{3}','{4}');\n".format(CALC_SCHEMA, specificationName, schemaName,sourceName,self.outputColumn))

    def getCustomQueueTable(self,sourceName):
        tableName = "{0}_custom_queue".format(sourceName)
        ddl = ( "CREATE TABLE {0}.{1} (\n"
                "  source_id bigint NOT NULL PRIMARY KEY);\n\n".format(CALC_SCHEMA, tableName))        
        return chimpsql.Table(tableName, CALC_SCHEMA, ddl)


    def getCustomQueueFunction(self, sourceName):
        functionName = "add_to_{0}_custom_queue".format(sourceName)
        return chimpsql.Function(functionName, CALC_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS trigger AS\n"
                         "$BODY$\n"
                         "DECLARE\n"
                         "  v_exists BOOLEAN;\n"
                         "BEGIN\n"
                         "  SELECT exists(SELECT 1 FROM {0}.{2}_custom_queue WHERE source_id = new.id)\n"
                         "  INTO v_exists;\n"
                         "  IF NOT v_exists THEN\n"
                         "    INSERT INTO {0}.{2}_custom_queue(\n"
                         "      source_id)\n"
                         "    VALUES (\n"
                         "      new.id);\n"
                         "  END IF;\n"
                         "  RETURN new;\n"                                    
                         "END;\n"
                         "$BODY$\n"
                         "LANGUAGE plpgsql;\n\n").format(CALC_SCHEMA, functionName, sourceName))


    def getCustomQueueInsertTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "j_add_{0}_insert_to_custom_queue".format(sourceName)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER INSERT\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, sourceSchema, sourceName, CALC_SCHEMA, triggerFunction.name))

    def getCustomQueueUpdateTrigger(self, sourceSchema, sourceName, triggerFunction, allTriggeringColumns):
        triggerName = "j_add_{0}_update_to_custom_queue".format(sourceName)
        ofClause = ", ".join(allTriggeringColumns)
        whenClause=" OR ".join(map(lambda field: "old.{0} IS DISTINCT FROM new.{0}".format(field), allTriggeringColumns))                
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER UPDATE\n"
                        "OF {1}\n"
                        "ON {2}.{3}\n"
                        "FOR EACH ROW\n"
                        "WHEN ({4})\n"                        
                        "EXECUTE PROCEDURE {5}.{6}();\n\n").format(triggerName, ofClause, sourceSchema, sourceName, whenClause, CALC_SCHEMA, triggerFunction.name))


    
    def getFunctionScript(self, source):
        body= ("\t# Function to derive contents of:\n"
               "\t#   {0}\n"
               "\tdef custom_{1}_calculator(self, dbCursor, data):\n".format(self.field.columnClause(False), self.outputColumn))
        
        body += self.templateAssembler.getScript(source)        
        return(body)          
    
    
    def getPinScript(self):
        body = self.templateAssembler.getPinScript(self.outputColumn)
        return(body)
    

def processSynchronizeCustomColumn(queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, args, processor):

    # Init
    
    lineCount = 0
    successCount = 0
    exceptionCount=0
    errorCount=0
    warningCount=0
    noticeCount=0
    ignoredCount=0   
    appLogger = settings.appLogger
    commitThreshold = int(settings.env["dataCommitThreshold"])
    
    appLogger.info("  |")

    inputSourceSchema = args["inputSourceSchema"]
    inputSourceName = args["inputSourceName"]
    customList = args["customList"] 
    flushQueue = args["flushQueue"]
            
    appLogger.info("  |{0}.{1}:".format(inputSourceSchema,inputSourceName))
    
    customColumnResults=[]
    calculatorCache={}
    appLogger.info("  |  Column list:")
    for custom in customList:    
        appLogger.info("  |    {0}".format(custom))
        calculatorCache[custom]=getattr(processor, "custom_{0}_calculator".format(custom))
        
    
    # Publish how many rows containing custom-content we're talking about
    queue.startTask(taskId, True)
    sql = "select count(*) from {0}.{1}_custom_queue".format(CALC_SCHEMA, inputSourceName)
    appLogger.info("  |    sql: {0}".format(sql))
    supportCursor.execute(sql)
    customCount = supportCursor.fetchone()[0]
    queue.setScanResults(taskId, customCount)
    appLogger.info("  |    customCount        : {0}".format(customCount))

    # Construct update statement
    setClause= ", ".join(map(lambda field: "{0}=%s".format(field), customList)) 
                                    #"".join(map(lambda field: ",\n  {0}".format(field.column), record.getAllMappedFields())), 


    updateDml = "update {0}.{1} set {2} where id = %s".format(inputSourceSchema, inputSourceName, setClause)

    deleteFromQueue = "delete from {0}.{1}_custom_queue where source_id=%s".format(CALC_SCHEMA, inputSourceName)

    # Establish main loop
    loopSql = "select b.* from {0}.{1}_custom_queue as a join {2}.{1} as b on (a.source_id=b.id)".format(CALC_SCHEMA, inputSourceName, inputSourceSchema)    
    loopCursor = loopConnection.makeCursor("custom", True, True)
    loopCursor.execute(loopSql)

    appLogger.info("  |    loopSql   : {0}".format(loopSql))
    appLogger.info("  |    updateDml : {0}".format(updateDml))
    appLogger.info("  |    deleteDml : {0} - flushQueue={1}".format(deleteFromQueue, flushQueue))
    recordCount=0

    for record in loopCursor:   
        recordCount+=1
        if recordCount%1000 ==0:
            queue.setTaskProgress(taskId, recordCount, 0, 0, 0, 0, 0)
        if recordCount % commitThreshold == 0:
            appLogger.debug("  |    << Transaction size threshold reached ({0}): COMMIT >>".format(recordCount))
            dataConnection.connection.commit()
                        
        params=[]
        for column in customList:        
            params.append(calculatorCache[column](dataCursor, record))
        
        id = record["id"]
        params.append(id)
        
        if recordCount < 11:
            appLogger.info('  |   [{0}] = Params: {1}'.format(id, params))

        dataCursor.execute(updateDml, tuple(params))
        if flushQueue:
            dataCursor.execute(deleteFromQueue, (id,))
        
        successCount += 1
        
    loopCursor.close()
    queue.finishTask(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
    supportConnection.connection.commit()

    return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )

