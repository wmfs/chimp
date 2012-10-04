'''
Created on 4 Mar 2012

@author: Tim.Needham
'''
PINHEAD_SCHEMA = "pinhead"
CALC_SCHEMA = "calc"
import cs
from calc.ContentAssembler import ContentAssembler
import chimpsql
import calc
import json
import os
import chimpspec

class Pin:
        
    def __init__(self, pinTag, settings):        
        self.type = "pin"
        self.taskOrder = 50
        self.triggeringColumns = []
        self.name= cs.grabAttribute(pinTag,"name")
        self.title= cs.grabAttribute(pinTag,"title")
        self.description= cs.grabAttribute(pinTag,"description")
        self.xColumn = cs.grabAttribute(pinTag,"xColumn")
        self.yColumn = cs.grabAttribute(pinTag,"yColumn")
        
        self.whereClause = cs.grabAttribute(pinTag,"whereClause")
        if self.whereClause is not None:
            self.whereClause = self.whereClause.replace("'","''")
        
        self.idColumn = cs.grabAttribute(pinTag,"idColumn")
        self.keyColumn = cs.grabAttribute(pinTag,"keyColumn")
        self.documentType = cs.grabAttribute(pinTag,"documentType")
        self.vicinityResultIconColumn = cs.grabAttribute(pinTag,"vicinityResultIconColumn")
        self.vicinityResultIconConstant = cs.grabAttribute(pinTag,"vicinityResultIconConstant")
        self.vicinityResultLabelColumn = cs.grabAttribute(pinTag,"vicinityResultLabelColumn")
        
        if self.idColumn is None:
            self.idColumn = "id"
        
        self.minimumVisibility = cs.grabAttribute(pinTag,"minimumVisibility")
        self.maximumSecurity = cs.grabAttribute(pinTag,"maximumSecurity")
        
        computedDataTag = pinTag.getElementsByTagName("computedData")
        self.computedData = calc.CalculatedData("??", computedDataTag, settings)
        
        self.additionalIndexes=[]
        additionalIndexesTag = pinTag.getElementsByTagName("additionalIndexes")
        if additionalIndexesTag.length > 0:
            additionalIndexesTag = additionalIndexesTag[0]
            indexesTag = additionalIndexesTag.getElementsByTagName("index")
            
            for thisIndex in indexesTag:
                self.additionalIndexes.append(chimpspec.AdditionalIndex(thisIndex))
        
    
                
 
    def getTriggeringColumns(self):
        columns=[]
        columns.append(self.xColumn)
        columns.append(self.yColumn)
        for element in self.computedData.elements:
            columns.extend(element.getTriggeringColumns())
        return(columns)                           
        
    def debug(self, appLogger):
        appLogger.debug("    pin")
        appLogger.debug("      name                      : {0}".format(self.name))
        appLogger.debug("      title                     : {0}".format(self.title))
        appLogger.debug("      keyColumn                 : {0}".format(self.keyColumn))
        appLogger.debug("      documentType              : {0}".format(self.documentType))
        appLogger.debug("      vicinityResultIconColumn  : {0}".format(self.vicinityResultIconColumn))
        appLogger.debug("      vicinityResultIconConstant: {0}".format(self.vicinityResultIconConstant))
        appLogger.debug("      vicinityResultLabelColumn : {0}".format(self.vicinityResultLabelColumn))
        appLogger.debug("      description               : {0}".format(self.description))
        appLogger.debug("      idColumn                  : {0}".format(self.idColumn))
        appLogger.debug("      xColumn                   : {0}".format(self.xColumn))
        appLogger.debug("      yColumn                   : {0}".format(self.yColumn))
        appLogger.debug("      whereClause               : {0}".format(self.whereClause))
        appLogger.debug("      minimumVisibility         : {0}".format(self.minimumVisibility))
        appLogger.debug("      maximumSecurity           : {0}".format(self.maximumSecurity))
        appLogger.debug("      additionalIndexes:")
        for index in self.additionalIndexes:
           index.debug(appLogger) 
            
        
        for element in self.computedData.elements:
            element.debug(appLogger)
                
    def getExtraSystemFields(self):
        extraSystemFields = []
        return(extraSystemFields)

    def requiresFile(self):
        return(True)  
        
    def getPinIdSequence(self):
        sequenceName = "{0}_seq".format(self.name)
        return chimpsql.Sequence(sequenceName, PINHEAD_SCHEMA, 
                        ("CREATE SEQUENCE {0}.{1}\n"
                         "INCREMENT 1\n"
                         "MINVALUE 1\n"
                         "START 10\n"
                         "CACHE 1;\n\n").format(PINHEAD_SCHEMA, sequenceName))
    
    def getInputColumns(self):
        inputColumns = []
        inputColumns.append(self.xColumn)
        inputColumns.append(self.yColumn)        
        for element in self.computedData.elements:              
            if element.type =="customColumn":                    
                for content in element.templateAssembler.contentElements:
                    inputColumns.append(content.column)
            elif element.type =="mappedColumn":
                inputColumns.append(element.inputColumn)
                
        # Only get distinct columns names, and ensure they're sorted alphabetically
        inputColumns=set(inputColumns)
        inputColumns=list(inputColumns)
        inputColumns = sorted(inputColumns)     
        return(inputColumns)

    def getOutputColumns(self):
        outputColumns = []
        outputColumns.append("pin_id")
        outputColumns.append("source_id")
        outputColumns.append("x")
        outputColumns.append("y")      
        for element in self.computedData.elements:              
                outputColumns.append(element.outputColumn)                                      
        return(outputColumns)
    
    def getPinheadPinRegistrationDML(self, schemaName, specificationName, sourceName, repositoryPath):
        inputColumnList = ",".join(self.getInputColumns())
        outputColumnList = ",".join(self.getOutputColumns())

        return chimpsql.DML(("SELECT {0}.register_pin('{1}', '{2}', '{3}', '{4}', "
                    "'{5}', '{6}', '{7}', '{8}', '{9}', '{10}', '{11}', {12});\n\n").format(CALC_SCHEMA, 
                                                 self.name, 
                                                 specificationName, 
                                                 self.title, 
                                                 self.description,
                                                 self.idColumn,
                                                 self.xColumn,
                                                 self.yColumn,
                                                 schemaName,
                                                 sourceName,
                                                 inputColumnList,
                                                 outputColumnList,
                                                 "NULL" if self.whereClause is None else "'{0}'".format(self.whereClause)),
                    dropDdl="SELECT {0}.unregister_pin('{1}');\n".format(CALC_SCHEMA, self.name))
    
    def getPinheadTable(self):        
        ddl = ( "CREATE TABLE {0}.{1} (\n"
                "  pin_id bigint primary key NOT NULL,\n"
                "  source_id bigint NOT NULL,"
                "  x int not null,\n"
                "  y int not null,\n".format(PINHEAD_SCHEMA, self.name) )        
        for element in self.computedData.elements:
            for field in element.getExtraSystemFields():
                ddl += "{0},\n".format(field.columnClause(None))        
        ddl += ("  visibility smallint,\n"
                "  security smallint,\n"
                "  last_refreshed timestamp with time zone not null default current_timestamp);\n\n")
        return chimpsql.Table(self.name, PINHEAD_SCHEMA, ddl)


    def getPinheadPinMaintainerFunction(self, srid):
        functionName = "{0}_pin_maintainer".format(self.name)
        return chimpsql.Function(functionName, PINHEAD_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS trigger AS\n"
                         "$BODY$\n"
                         "BEGIN\n"
                         "  new.pin = ST_GeometryFromText('POINT('||new.x::character varying||' '||new.y::character varying||')',{2});\n"
                         "  RETURN new;\n"                                    
                         "END;\n"
                         "$BODY$\n"
                         "LANGUAGE plpgsql;\n\n").format(PINHEAD_SCHEMA, functionName, srid))
        
    def getPinheadMaintainerInsertTrigger(self, triggerFunction):
        triggerName = "maintain_{0}_pin_before_insert".format(self.name)
        return chimpsql.Trigger(triggerName, self.name, triggerFunction.name, PINHEAD_SCHEMA,
                       ("CREATE TRIGGER {0}\n"
                        "BEFORE INSERT\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {1}.{3}();\n\n").format(triggerName, PINHEAD_SCHEMA, self.name, triggerFunction.name))

    def getPinheadMaintainerUpdateTrigger(self, triggerFunction):
        triggerName = "maintain_{0}_pin_before_update".format(self.name)
        return chimpsql.Trigger(triggerName, self.name, triggerFunction.name, PINHEAD_SCHEMA,
                       ("CREATE TRIGGER {0}\n"
                        "BEFORE UPDATE OF x,y\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "WHEN (old.x IS DISTINCT FROM new.x OR old.y IS DISTINCT FROM new.y)\n"
                        "EXECUTE PROCEDURE {1}.{3}();\n\n").format(triggerName, PINHEAD_SCHEMA, self.name, triggerFunction.name))

    def getPinheadExistsFunction(self, table):
        functionName = "{0}_exists".format(self.name)
        return chimpsql.Function(functionName, PINHEAD_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS boolean AS $$\n"
                         "DECLARE\n"
                         "  r BOOLEAN;\n"
                         "BEGIN\n"
                         "  SELECT exists(select 1 from {2}.{3} limit 1)\n"
                         "  INTO r;\n"
                         "  RETURN r;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(PINHEAD_SCHEMA, functionName, table.schema, table.name))
    
    def getInAreaFunction(self):
        functionName = "get_{0}_pins_in_area".format(self.name)

        if self.keyColumn is not None:
            keyColumn = self.keyColumn
        else:
            keyColumn = "null"

        if self.documentType is not None:
            documentType = "'{0}'".format(self.documentType)
        else:
            documentType = "null"   

        if self.vicinityResultIconColumn is not None:
            vicinityResultIcon = self.vicinityResultIconColumn
        elif self.vicinityResultIconConstant is not None:
            vicinityResultIcon = "''{0}''".format(self.vicinityResultIconConstant)
        else:
            vicinityResultIcon ="null"

        if self.vicinityResultLabelColumn is not None:            
            vicinityResultLabelColumn = self.vicinityResultLabelColumn
        else:
            vicinityResultLabelColumn = None
            
        dml = ("CREATE OR REPLACE FUNCTION {0}.{1}(p_geometry geometry, p_where character varying = NULL)\n"
               "RETURNS SETOF {0}.pin_result AS\n"
               " $BODY$\n"
               " DECLARE\n"
               "   v_sql character varying(2000);\n"
               "   v_result {0}.pin_result;\n"
               "   this_pin record;\n"
               " BEGIN\n"
               "   v_sql = 'select {2} as document_key,{3},{4},{6} as vicinity_label,{7} as vicinity_icon,visibility,security FROM pinhead.{5} WHERE ST_Within(pin, $1)';\n"
               "   IF p_where IS NOT NULL THEN\n"
               "     v_sql = v_sql || ' AND (' || p_where ||')';\n"
               "   END IF;\n"
               "   FOR this_pin IN EXECUTE v_sql USING p_geometry LOOP\n"               
               "     v_result.pin_name = '{5}';\n"
               "     v_result.key= this_pin.document_key::character varying;\n"
               "     v_result.document_type= {8};\n"
               "     v_result.x = this_pin.{3};\n"
               "     v_result.y = this_pin.{4};\n"
               "     v_result.icon = this_pin.vicinity_icon;\n"
               "     v_result.label = substring(this_pin.vicinity_label,1,200);\n"
               "     v_result.visibility = this_pin.visibility;\n"
               "     v_result.security = this_pin.security;\n"
               "     RETURN NEXT v_result;\n"
               "   END LOOP;\n"
               " END;\n"
               " $BODY$\n"
               "LANGUAGE plpgsql VOLATILE;\n\n")
        return chimpsql.Function(functionName, PINHEAD_SCHEMA, [],
                        dml.format(PINHEAD_SCHEMA, functionName, keyColumn, "x", "y", self.name, vicinityResultLabelColumn, vicinityResultIcon, documentType ))
        
        
            
    def getPinheadGeometryAddDML(self, srid):
        return chimpsql.DML("SELECT AddGeometryColumn('pinhead', '{0}', 'pin', {1}, 'GEOMETRY', 2);\n\n".format(self.name, srid))

    def getPinheadSpatialIndex(self):
        indexName = "{0}_pin_idx".format(self.name)
        return chimpsql.Index(indexName, self.name, PINHEAD_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} USING gist(pin);\n\n".format(indexName, PINHEAD_SCHEMA, self.name))   

    def getPinheadSourceIdIndex(self):
        indexName = "{0}_pin_source_id_idx".format(self.name)
        return chimpsql.Index(indexName, self.name, PINHEAD_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} (source_id);\n\n".format(indexName, PINHEAD_SCHEMA, self.name), droppable=False)   

    def getAdditionalPinIndex(self, index,  pinheadTable):
        indexName = "{0}_{1}".format(self.name, index.underscoreDelimitedColumns)
        return chimpsql.Index(indexName, pinheadTable.name, pinheadTable.schema,
                     "CREATE INDEX {0} ON {1}.{2}{3}({4});\n\n".format(indexName, pinheadTable.schema, pinheadTable.name,
                                                                       "" if index.using is None else " USING {0} ".format(index.using),
                                                                       index.commaDelimitedColumns))
        

    def getPinheadQueueTable(self):
        tableName = "{0}_pins_queue".format(self.name)
        ddl = ( "CREATE TABLE {0}.{1} (\n"
                "  source_id bigint NOT NULL PRIMARY KEY);\n\n".format(CALC_SCHEMA, tableName, self.name))        
        return chimpsql.Table(tableName, CALC_SCHEMA, ddl)

#    def getPinheadQueueTableIndex(self):
#        indexName = "{0}_queue_id".format(self.name)
#        tableName = "{0}_queue".format(self.name)
#        return chimpsql.Index(indexName, tableName, PINHEAD_SCHEMA,
#                     "CREATE INDEX {0} ON {1}.{2}(source_id);\n\n".format(indexName, PINHEAD_SCHEMA, tableName))   

#    def getPinheadQueueView(self,sourceSchema, sourceName):
#        viewName = "incoming_{0}_changes".format(self.name)
#        inputColumnList = ",".join(self.getInputColumns())
#
#        return chimpsql.View(viewName, PINHEAD_SCHEMA,(
#                        "CREATE OR REPLACE VIEW {0}.{1} AS\n"
#                        "SELECT pin_id,{2},needs_delete_first,{3}\n" 
#                        "FROM {0}.{4}_queue as a\n"
#                        "LEFT JOIN {5}.{6} as b on a.source_id = b.{2};\n\n".format(PINHEAD_SCHEMA, viewName, self.idColumn, inputColumnList,  self.name, sourceSchema, sourceName)))  

    def getPinheadQueueFunction(self, dmlStatement, records):
        functionName = "add_to_{0}_pinhead_queue_{1}".format(self.name, dmlStatement)
        ddl = ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                             "  RETURNS trigger AS\n"
                             "$BODY$\n"
                             "DECLARE\n"
                             "  v_exists BOOLEAN;\n"
                             "BEGIN\n").format(CALC_SCHEMA, functionName)
        for record in records:
            ddl += ("  SELECT exists(SELECT 1 FROM {0}.{2}_pins_queue WHERE source_id={3}.{4})\n"
                    "  INTO v_exists;\n"
                    "  IF NOT v_exists THEN\n"
                    "    INSERT INTO {0}.{2}_pins_queue (source_id)\n"
                    "    VALUES ({3}.{4});\n"
                    "  END IF;\n").format(CALC_SCHEMA, functionName, self.name, record,self.idColumn)

        ddl += ("  RETURN new;\n"                                    
                "END;\n"
                "$BODY$\n"
                "LANGUAGE plpgsql;\n\n")

        
        return chimpsql.Function(functionName, CALC_SCHEMA, [],ddl)
#           
#                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
#                         "  RETURNS trigger AS\n"
#                         "$BODY$\n"
#                         "DECLARE\n"
#                         "  v_exists BOOLEAN;\n"
#                         "BEGIN\n"
#                         "  SELECT exists(SELECT 1 FROM {0}.{2}_pins_queue WHERE source_id = new.{3})\n"
#                         "  INTO v_exists;\n"
#                         "  IF NOT v_exists THEN\n"
#                         "    INSERT INTO {0}.{2}_pins_queue(\n"
#                         "      source_id)\n"
#                         "    VALUES (\n"
#                         "      new.{3});\n"
#                         "  END IF;\n"
#                         "  RETURN new;\n"                                    
#                         "END;\n"
#                         "$BODY$\n"
#                         "LANGUAGE plpgsql;\n\n").format(CALC_SCHEMA, functionName, self.name, self.idColumn))

    def getPinheadQueueInsertTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "m_add_{0}_insert_to_pinhead_queue".format(self.name)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER INSERT\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, sourceSchema, sourceName, CALC_SCHEMA, triggerFunction.name))

    def getPinheadQueueUpdateTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "m_add_{0}_update_to_pinhead_queue".format(self.name)
        triggeringColumns = list(set(self.getTriggeringColumns()))
        ofClause = ", ".join(triggeringColumns)
        whenClause=" OR ".join(map(lambda field: "old.{0} IS DISTINCT FROM new.{0}".format(field), triggeringColumns))
            
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER UPDATE\n"
                        "OF {1}\n"
                        "ON {2}.{3}\n"
                        "FOR EACH ROW\n"
                        "WHEN ({4})\n"
                        "EXECUTE PROCEDURE {5}.{6}();\n\n").format(triggerName, ofClause, sourceSchema, sourceName, whenClause, CALC_SCHEMA, triggerFunction.name))

    def getPinheadQueueDeleteTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "m_add_{0}_delete_to_pinhead_queue".format(self.name)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER DELETE\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, sourceSchema, sourceName, CALC_SCHEMA, triggerFunction.name))

    def getFunctionScript(self, sourceFields):
        body= ("\t# Function to generate the contents for a '{0}' pin\n"
               "\tdef {0}_pin_calculator(self, dbCursor, data):\n"
               "\t\tr=[]\n"
               '\t\tr.append(data["pin_id"]) # --> pin_id\n'
               '\t\tr.append(data["{1}"]) # --> source_id\n'
               '\t\tr.append(data["{2}"]) # --> x\n'
               '\t\tr.append(data["{3}"]) # --> y\n'                              
               .format(self.name, self.idColumn, self.xColumn, self.yColumn))
        
        for element in self.computedData.elements:
            body += element.getPinScript()
        body+= "\t\treturn(tuple(r))\n\n"

        return(body)          

        # OLD PINHEAD
#        if settings.args.chainpinhead:
#            for thisRecord in settings.specification.records:
#                if thisRecord.useful:
#                    for thisPin in thisRecord.pins:                    
#                        sql = "select pinhead.%s_exists()" %(thisPin.name)
#                        self.supportCursor.execute(sql)        
#                        pinsExist = self.supportCursor.fetchone()[0]                    
#                        if not pinsExist:
#                            args = {}
#                            filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specification files",specificationName,"sql","indexes"), "drop_pinhead_%s_indexes.sql" % (thisPin.name))                                                                                    
#                            args["filename"] = filename
#                            self.queue.queueTask(groupId,  stream,  "script" , "Drop %s pin indexes" %(thisPin.name), None, None, None, json.dumps(args), False)                                                        
#                            self.queue.queueCheckpoint(groupId, stream, "major", settings.args.tolerancelevel, commitFrequency, checkpointBehaviour)
#                        args = {}
#                        args["pinName"] = thisPin.name
#                        self.queue.queueTask(groupId, stream,  "syncPins", "Refresh %s pins" %(thisPin.name), None, None, None, json.dumps(args), False)
#                        if not pinsExist:
#                            args = {}
#                            filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specification files",specificationName,"sql","indexes"), "create_pinhead_%s_indexes.sql" % (thisPin.name))                                                                                    
#                            args["filename"] = filename
#                            self.queue.queueTask(groupId,  stream, "script" , "Build %s pin indexes" %(thisPin.name), None, None, None, json.dumps(args), False)            
#    
#                        self.queue.queueCheckpoint(groupId, stream, "major", settings.args.tolerancelevel, commitFrequency, checkpointBehaviour)


def processSynchronizePins(queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, args, processor):

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
        
    appLogger.info("")
    appLogger.info("processSynchronizePins")
    appLogger.info("----------------------")
    
    pinName = args["pinName"]
    inputIdColumn = args["inputIdColumn"]
    inputXColumn = args["inputXColumn"]
    inputYColumn = args["inputYColumn"]
    inputSchema = args["inputSchema"]
    inputSource = args["inputSourceName"]
    inputColumnList = args["inputColumnList"]
    outputColumnList = args["outputColumnList"]
    whereClause = args["whereClause"]
    
    appLogger.info("")
    appLogger.info(" {0}:".format(pinName))
    appLogger.debug("  inputIdColumn     : {0}".format(inputIdColumn))
    appLogger.debug("  inputXColumn      : {0}".format(inputXColumn))
    appLogger.debug("  inputYColumn      : {0}".format(inputYColumn))
    appLogger.debug("  inputSchema       : {0}".format(inputSchema))
    appLogger.debug("  inputSource       : {0}".format(inputSource))
    appLogger.debug("  inputColumnList   : {0}".format(inputColumnList))
    appLogger.debug("  outputColumnList  : {0}".format(outputColumnList))
    appLogger.debug("  whereClause       : {0}".format(whereClause))
    
        
    
    # Publish how many pins we're talking about
    queue.startTask(taskId, True)
    sql = "select count(*) from {0}.{1}_pins_queue".format(CALC_SCHEMA, pinName)
    supportCursor.execute(sql)
    pinCount = supportCursor.fetchone()[0]
    queue.setScanResults(taskId, pinCount)
    appLogger.info("   pinCount          : {0}".format(pinCount))
    
    # Grab the method that will do all the formatting    
    outputMethod = getattr(processor, "{0}_pin_calculator".format(pinName))

    # Establish main loop
    loopSql = ("select source_id from {0}.{1}_pins_queue".format(CALC_SCHEMA, pinName))    
    loopCursor = loopConnection.makeCursor(None, False, False)

       
    # Create new pins SQL
    newPinsSql = "select nextval('{0}.{1}_seq') as pin_id,{2},{3} from {4}.{5} where ({6}=%s) AND ({7} IS NOT NULL AND {8} IS NOT NULL)".format(PINHEAD_SCHEMA,pinName,inputIdColumn,inputColumnList,inputSchema,inputSource,inputIdColumn, inputXColumn, inputYColumn)
    appLogger.debug("  newPinsSql        : {0}".format(newPinsSql))
    newPinCursor = dataConnection.makeCursor(None, False, True)
    
    # Create insert DML    
    placeHolders="%s"
    i = 0
    while i<outputColumnList.count(","):
        placeHolders += ",%s"
        i += 1    
    insert = "insert into {0}.{1}({2}) values ({3})".format(PINHEAD_SCHEMA, pinName, outputColumnList,placeHolders)
    appLogger.debug("  insertSql         : {0}".format(insert))

    # Create delete pin DML
    deleteExistingPins = "delete from {0}.{1} where source_id=%s".format(PINHEAD_SCHEMA, pinName)

    # Create delete from queue DML
    deleteFromQueue = "delete from {0}.{1}_pins_queue where source_id=%s".format(CALC_SCHEMA, pinName)


    loopCursor.execute(loopSql)
    recordCount=0
    for record in loopCursor:
        
        recordCount+=1
        if recordCount%1000 ==0:
            queue.setTaskProgress(taskId, recordCount, 0, 0, 0, 0, 0)
        if recordCount % commitThreshold == 0:
            appLogger.debug("| << Transaction size threshold reached ({0}): COMMIT >>".format(recordCount))
            dataConnection.connection.commit()
        
        # Delete any pins that might be there already
        dataCursor.execute(deleteExistingPins, (record[0],))
        
        # Insert new pins
        newPinCursor.execute(newPinsSql, (record[0],))
        for newPin in newPinCursor:
            output = outputMethod(dataCursor, newPin)
            dataCursor.execute(insert, output)
        
        # Delete from queue
        dataCursor.execute(deleteFromQueue, (record[0],))
        
        lineCount += 1

#        output = outputMethod(dataCursor, record)
#        dataCursor.execute(insert, output)
#    
    queue.finishTask(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
    supportConnection.connection.commit()
    
    loopCursor.close()



#    
#    
#    for thisRecord in specification.records:            
#        if thisRecord.table == sourceTable:                
#            for thisPin in thisRecord.pins:
#                if thisPin.name==pinName: 
#
#                    # Construct SQL/DML
#                    # =================
#                    dmlColumnCount=7
#                    selectList=[]
#                    paramList = []
#
#                    selectList.append("a.id")                            
#                    for thisPrimaryKeyColumn in thisRecord.primaryKeyColumns:
#                        selectList.append("a."+thisPrimaryKeyColumn)
#                        dmlColumnCount = dmlColumnCount + 1
#                                                    
#                    for additionalColumn in thisPin.additionalPinColumns:
#                        selectList.append("a."+additionalColumn)
#                        dmlColumnCount = dmlColumnCount + 1
#
#                    selectList.append("a." + thisPin.xColumn)
#                    selectList.append("a." + thisPin.yColumn)
#
#                    for thisGenerator in thisPin.iconGenerators:
#                        dmlColumnCount = dmlColumnCount + 2
#                        if thisGenerator.simpleMappingSourceColumn is not None:
#                            i=0
#                            for thisColumn in selectList:
#                                if thisColumn == "a.%s" %(thisGenerator.simpleMappingSourceColumn):
#                                    thisGenerator.setColumnIndex(i)
#                                i=i+1
#                    
#                    selectList.append("p.x" )
#                    selectList.append("p.y")                        
#                    selectList.append("coalesce(a.visibility,%s.get_%s_default_visibility())" %(sourceSchema,sourceTable))
#                    selectList.append("coalesce(a.security,%s.get_%s_default_security())" %(sourceSchema,sourceTable))
#                     
#                    sql = "select " + cs.delimitedStringList(selectList, ",")
#                    sql = sql + " from %s.%s AS a" %(sourceSchema, sourceTable)                                                                                 
#                    sql=sql+" LEFT JOIN pinhead.%s as p ON(a.id=p.id)" %(pinName)
#                    
#                    if lastSynchronized is not None:
#                        sql=sql+" WHERE a.%s_pin_modified > " %(pinName)
#                        sql=sql+"%s"
#                        loopCursor.execute(sql, (lastSynchronized,))
#                    else:
#                        loopCursor.execute(sql)
#
#                    # Build DML
#                    dml = "select pinhead.synchronize_%s_pin(" %(pinName)
#                    for i in range(0,dmlColumnCount):
#                        if i>0:
#                            dml=dml+","
#                        dml=dml+"%s"
#                    dml=dml+")"
#                    
#                    # Main loop                                                
#                    lineCount = 0
#                    successCount = 0
#                    exceptionCount=0
#                    errorCount=0
#                    warningCount=0
#                    noticeCount=0
#                    ignoredCount=0   
#                    
#                                           
#                    for data in loopCursor:                            
#                        if lineCount % 1000 == 0:                
#                            queue.setTaskProgress(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)
#                        lineCount = lineCount + 1                            
#                        
#                        workingData = list(data)
#                        
#                        for thisGenerator in thisPin.iconGenerators:             
#                            
#                            if thisGenerator.simpleMappingSourceColumn is not None:
#                                                   
#                                sourceColumnData = data[thisGenerator.simpleMappingSourceColumnIndex]                                
#                                
#                                if str(sourceColumnData) in thisGenerator.mapping:
#                                    # Value can be mapped to a filename/priority
#                                    mappedValues = thisGenerator.mapping[str(sourceColumnData)]
#                                    workingData.append(mappedValues[0])
#                                    workingData.append(mappedValues[1])
#                                else:
#                                    # Not in mapping list, but has defaults so...
#                                    if thisGenerator.simpleMappingUnmappedFilename is not None and thisGenerator.simpleMappingUnmappedPriority is not None:
#                                        workingData.append(thisGenerator.simpleMappingUnmappedFilename)
#                                        workingData.append(thisGenerator.simpleMappingUnmappedPriority)
#                                    else:
#                                        # Not mapped, no defaults... clear it off.
#                                        workingData.append(None)
#                                        workingData.append(None)                                            
#                            else:                                    
#                                workingData.append(thisGenerator.fixedFilename)
#                                workingData.append(None)                           
#
#                        
#                        try:
#                            # Apply DML statement
#                            dataCursor.execute(dml, tuple(workingData))                            
#                            successCount = successCount + 1
#                            
#                        except Exception as detail:
#                            try:
#                                exceptionCount = exceptionCount + 1
##                                    dataConnection.connection.rollback()                                    
#                                queue.addTaskMessage(taskId, "%s.%s" %(sourceSchema,sourceTable), lineCount, "exception", "EXP", "Exception attempting synchronization of %s pin" %(pinName), None, 1, "Message:\n%s\n\nData:\n%s"  %(str(detail), str(data)))
#
#                            except Exception as subDetail:
#                                print ("Error")
#                                print (detail)
#                                print ("Failed to log error:")
#                                print (subDetail)
#                                raise
#
#                    #chimpqueue.finishTask(supportConnection, supportCursor, taskId, True, True, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)                    
#                                                                                

    return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )



# ----------------------------------------------------------------------------------

#        return chimpsql.Table(self.name, PINHEAD_SCHEMA,
#                     ("CREATE TABLE {0}.{1} (\n"
#                        "  id bigint primary key{2}{3},\n"
#                        "  x int not null,\n"
#                        "  y int not null{4},\n"
#                        "  visibility smallint,\n"
#                        "  security smallint,\n"
#                        "  created timestamp with time zone not null default current_timestamp,\n"
#                        "  modified timestamp with time zone not null default current_timestamp)\n"
#                        "WITH (OIDS=TRUE);\n\n").format(PINHEAD_SCHEMA,self.name,
#                                                        self._getColumnDefsSQL(record.getPrimaryKeyFields()),
#                                                        "".join(map(lambda icon: (",\n"
#                                                                                  "  {0}_filename character varying(60),\n"
#                                                                                  "  {0}_priority integer default 100").format(icon.name), "???")),
#                                                        self._getColumnDefsSQL(record.getAdditionalPinheadFields(self))))   
        
#    def getPinheadPrimaryKeyColumnsIndex(self,  record, table):
#        indexName = "{0}_pk_columns".format(self.name)
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE UNIQUE INDEX {0} ON {1}.{2} ({3});\n".format(indexName, PINHEAD_SCHEMA, table.name, ",".join(record.primaryKeyColumns)))
    
#    def getPinheadModifiedIndex(self,  record, table):
#        indexName = "{0}_modified".format(self.name)                            
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE INDEX {0} ON {1}.{2} (modified);\n".format(indexName, PINHEAD_SCHEMA, table.name))
#    
#    def getPinheadVisibilityIndex(self,  record, table):
#        indexName = "{0}_visibility".format(self.name)                            
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE INDEX {0} ON {1}.{2} (visibility);\n".format(indexName, PINHEAD_SCHEMA, table.name))
#    
#    def getPinheadSecurityIndex(self,  record, table):
#        indexName = "{0}_security".format(self.name)
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE INDEX {0} ON {1}.{2} (security);\n".format(indexName, PINHEAD_SCHEMA, table.name))
#    
#    def getPinheadPinIndexIndex(self,  record, table):
#        indexName = "{0}_pin_idx".format(self.name)
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE INDEX {0} ON {1}.{2} USING gist (pin);\n".format(indexName, PINHEAD_SCHEMA, table.name))         
#    
    

    def getPinheadInsertProcessorTriggerFunction(self, srid):
        functionName = "{0}_insert_processor".format(self.name)
        return chimpsql.Function(functionName, PINHEAD_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
                         "BEGIN\n""  new.pin = ST_GeometryFromText('POINT('||new.x::character varying||' '||new.y::character varying||')',{2});\n"
                         "  RETURN new;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(PINHEAD_SCHEMA, functionName, srid))

    
    def getPinheadInsertProcessorTrigger(self, table, delegateFunction):
        triggerName = "{0}_insert".format(self.name)
        return chimpsql.Trigger(triggerName, table.name, delegateFunction.name, table.schema,
                       ("CREATE TRIGGER {0}\n" 
                        "BEFORE INSERT ON {1}.{2} FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, table.schema, table.name,
                                                                   delegateFunction.schema, delegateFunction.name))

    
    def getPinheadUpdateProcessorTriggerFunction(self,  srid):
        functionName = "{0}_update_processor".format(self.name)
        return chimpsql.Function(functionName, PINHEAD_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
                         "DECLARE\n"
                         "  v_modified BOOLEAN;\n"
                         "BEGIN\n"
                         "  v_modified = FALSE;\n"
                         "  IF (old.x != new.x) or (old.y != new.y) THEN\n"
                         "  new.pin = ST_GeometryFromText('POINT('||new.x::character varying||' '||new.y::character varying||')',{2});\n"
                         "    v_modified = TRUE;\n"
                         "  END IF;\n"
                         "  IF v_modified THEN\n"
                         "    new.modified = current_timestamp;\n"
                         "  END IF;\n"
                         "  RETURN new;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(PINHEAD_SCHEMA, functionName, srid))

    
    def getPinheadUpdateProcessorTrigger(self, table, delegateFunction):
        triggerName = "{0}_update".format(self.name)
        return chimpsql.Trigger(triggerName, PINHEAD_SCHEMA, delegateFunction.name, table.schema,
                        ("CREATE TRIGGER {0}\n"
                         "BEFORE UPDATE ON {1}.{2} FOR EACH ROW\n"
                         "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, table.schema, table.name,
                                                                   delegateFunction.schema, delegateFunction.name))

    def getPinheadDomainSourceRegistrationDML(self, record):
        return chimpsql.DML(("SELECT search.register_domain_source('{0}', 'table', '{1}', '{2}', "
                    "'{3}', TRUE);\n\n").format(record.search.searchDomain, record.getWorkingTargetSchema(), 
                                                record.table, self.specification.name),
                   dropDdl=("SELECT search.unregister_domain_source('{0}', '{1}', '{2}')"
                           ";\n").format(record.search.searchDomain, record.getWorkingTargetSchema(), record.table))
    
#
#    def buildPinheadSchema():
#
#        for record in specification.getUsefulRecords():
#            for pin in record.pins:
#
#                registerAndWrite(sqlBuilder.getPinheadPinRegistrationDML(pin, record), objectRegistry, file)
#
#                pinheadTable = sqlBuilder.getPinheadTable(pin, record)
#                registerAndWrite(pinheadTable, objectRegistry, file)
#
#                registerAndWrite(sqlBuilder.getPinheadGeometryAddDML(pin, srid), objectRegistry, file)
#
#                if record.hasPrimaryKey():
#                    registerAndWrite(sqlBuilder.getPinheadPrimaryKeyColumnsIndex(pin, record, pinheadTable), objectRegistry, file)
#
#                registerAndWrite(sqlBuilder.getPinheadModifiedIndex(pin, record, pinheadTable), objectRegistry, file)
#                registerAndWrite(sqlBuilder.getPinheadVisibilityIndex(pin, record, pinheadTable), objectRegistry, file)
#                registerAndWrite(sqlBuilder.getPinheadSecurityIndex(pin, record, pinheadTable), objectRegistry, file)
#                registerAndWrite(sqlBuilder.getPinheadPinIndexIndex(pin, record, pinheadTable), objectRegistry, file)
#
#                registerAndWrite(sqlBuilder.getPinheadExistsFunction(pin, pinheadTable), objectRegistry, file)
#
#                insertProcessorFunction = sqlBuilder.getPinheadInsertProcessorTriggerFunction(pin, srid)
#                registerAndWrite(insertProcessorFunction, objectRegistry, file)
#                registerAndWrite(sqlBuilder.getPinheadInsertProcessorTrigger(pin, pinheadTable, insertProcessorFunction), objectRegistry, file)
#                
#                updateProcessorFunction = sqlBuilder.getPinheadUpdateProcessorTriggerFunction(pin, srid)
#                registerAndWrite(updateProcessorFunction, objectRegistry, file)
#                registerAndWrite(sqlBuilder.getPinheadUpdateProcessorTrigger(pin, pinheadTable, updateProcessorFunction), objectRegistry, file)
#
#            if record.search.enabled:
#                registerAndWrite(sqlBuilder.getPinheadDomainSourceRegistrationDML(record), objectRegistry, file)
#
#                # TODO: refactor search module                                                   
#                sd = search.SearchDomain()
#                sd.setFromFile(settings, record.search.searchDomain)
#                sd.buildRecordFormatterScripts(settings, specification.name, record.table, record.search, record.search.searchDomain, appLogger)
#                
#                sd.writeDomainFunctions(file, record.getWorkingTargetSchema(), "table", record.table, record.search)
#                sd.debug(settings)
#     