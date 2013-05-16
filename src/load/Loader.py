import psycopg2
import os
import imp
import chimpExternalLoader
from load.Stager import Stager
import cs

class Loader:     
    
    def __init__(self, supportConnection, supportCursor, settings, queue):
        self.supportConnection = supportConnection
        self.supportCursor = supportCursor
        self.queue = queue
        self.settings = settings



    def makeEditableFile(self, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, specification, args):
        table = args["table"]
        appLogger = settings.appLogger            
        self.queue.startTask(taskId, True)        

        appLogger.debug("| {0}:".format(table))

        # Any editable data here already?
        # ===============================
        sql = "select exists (select 1 from editable.{0} limit 1)".format(table)
        self.supportCursor.execute(sql)
        dataExists = self.supportCursor.fetchone() 
        dataExists = dataExists[0]
        appLogger.debug("| dataExists: {0}".format(dataExists))


        # Get current timestamp
        # =====================
        sql = "select now()"
        self.supportCursor.execute(sql)
        thisImportStartTimestamp = self.supportCursor.fetchone()[0]
        appLogger.debug("| thisImportStartTimestamp : {0}".format(thisImportStartTimestamp))

        # Get last time schemas synchronised
        # ==================================
        sql = "select last_sent_to_editable from shared.specification_registry where name=%s"
        self.supportCursor.execute(sql, (specification.name,))
        lastImportTimestamp = self.supportCursor.fetchone()[0]    
        appLogger.debug("| lastImportTimestamp      : {0}".format(lastImportTimestamp))

        # Scanning
        # ========
        appLogger.debug("|  Scanning")
        #   Modified
        scanSql = "select count(*) from import.{0}" .format(table)
        if lastImportTimestamp is not None:            
            scanSql += " where modified >%s"
            self.supportCursor.execute(scanSql, (lastImportTimestamp,))
        else:
            self.supportCursor.execute(scanSql)                                        
        modifiedCount = self.supportCursor.fetchone()[0]
        appLogger.debug("|     Modified = {0}".format(modifiedCount))

        scanSql = "select count(*) from history.import_{0}_deletes" .format(table)
        if lastImportTimestamp is not None:            
            scanSql += " where deleted >%s"
            self.supportCursor.execute(scanSql, (lastImportTimestamp,))
        else:
            self.supportCursor.execute(scanSql)                                        
        deletedCount = self.supportCursor.fetchone()[0]
        appLogger.debug("|     Deleted  = {0}".format(deletedCount))
        totalCount = modifiedCount + deletedCount
        appLogger.debug("|                {0}".format(totalCount))        
        self.queue.setScanResults(taskId, totalCount)


        # Grab transformer function
        # =========================           
        moduleFilename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications", specification.name, "resources", "py", "transformation","editable"), "%s_editable_transformer.py" %(table))            
        module = imp.load_source("%s_editable_transformer.py" %(table), moduleFilename)
        transformFunction = module.transformSuppliedValues              
          
        # Establish files
        # ===============
        filename = os.path.join(settings.env["tempPath"], "insert_into_editable_{0}.sql".format(table))
        appLogger.debug("|")        
        appLogger.debug("| Filename: {0}".format(filename))
        insertFile = open(filename,"w")
                
        # Calculate DML placeholders
        # ==========================
        insertDml = "execute editable.{0}_insert(%s,%s".format(table)
        i=args["selectListLength"]
        while i>0:
            insertDml += ",%s"
            i-=1
        insertDml += ',"import");'
        appLogger.debug("| insertDml : {0}".format(insertDml))
        
        
        loopSql = "select {0} from import.{1}".format(args["selectList"], table)
        
        
        
        loopCursor = loopConnection.makeCursor("loopCursor", True, True)            
        loopCursor.execute(loopSql)

    
        lineCount = 0
        successCount = 0
        exceptionCount=0
        errorCount = 0
        warningCount = 0
        noticeCount = 0
        ignoredCount = 0        


        if not dataExists:
            for data in loopCursor:
                if lineCount % 1000 == 0:                
                    self.queue.setTaskProgress(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)
                lineCount = lineCount + 1
        
                transformedValues = transformFunction(dataCursor, data)
                quoted = str(psycopg2.extensions.adapt(transformedValues).getquoted())
                quoted = quoted[8:-2]
                
                line ="select editable.{0}_insert({1}, 'import');\n".format(table, quoted)
                insertFile.write(line)
                successCount+=1
                #line = self.supportCursor.mogrify(insertDml,transformedValues)
        
        insertFile.close()
        loopCursor.close()                        
        appLogger.debug("| Finished.")
        self.supportConnection.connection.commit()
        return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )
    
    #def processSendToEditable(dictionaryConnection, dictionaryCursor, inserterConnection, inserterCursor, namedConnection, namedCursor, settings, taskId, groupId, processLimit, args):
    def processFinishEditable(self, queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, args, recordedTimestamp):
        # Init
        lineCount = 0
        successCount = 1
        exceptionCount=0
        errorCount=0
        warningCount=0
        noticeCount=0
        ignoredCount=0   
        appLogger = settings.appLogger
    
        specification=args["specification"]
        sql = "update shared.specification_registry set last_sent_to_editable=%s where name=%s"
        appLogger.debug("| recordedTimestamp : {0}".format(recordedTimestamp))
        appLogger.debug("| specification     : {0}".format(specification))
        supportCursor.execute(sql, (recordedTimestamp, specification))
        supportConnection.connection.commit()
    
        queue.finishTask(taskId, 1, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
        return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )

    
    def processSendToEditable(self, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, specification, args):

        commitThreshold = int(settings.env["dataCommitThreshold"])
        appLogger = settings.appLogger
            
        self.queue.startTask(taskId, True)


        
        # Get last time schemas synchronised
        sql = "select last_sent_to_editable from shared.specification_registry where name=%s"
        self.supportCursor.execute(sql, (specification.name,))
        lastImportTimestamp = self.supportCursor.fetchone()[0]
    
        appLogger.debug("| lastImportTimestamp      : {0}".format(lastImportTimestamp))


        # Grab record
        table = args["table"]
        for r in specification.records:
            if r.table == table:
                thisRecord = r

        
    
        # Scanning 
        # ========   
        affectedRecordCount = 0    
        appLogger.debug("|   Scanning {0}:".format(table))
        
        # Count records that have been inserted/updated
        if lastImportTimestamp is None:            
            sql = "select count(*) from import.%s" %(table)
            self.supportCursor.execute(sql)
        else:
            sql = "select count(*) from import.%s where modified >" %(table)
            sql = sql + "%s"
            self.supportCursor.execute(sql, (lastImportTimestamp,))                            
        recordsModified = self.supportCursor.fetchone()[0]
        appLogger.debug("|     {0} (modified)".format(recordsModified))
        affectedRecordCount = affectedRecordCount + recordsModified           
        
        # Count records that have been deleted
        if lastImportTimestamp is None:            
            sql = "select count(*) from history.import_%s_deletes" %(table)
            self.supportCursor.execute(sql)
        else:
            sql = "select count(*) from history.import_%s_deletes where deleted >" %(table)
            sql = sql + "%s"
            self.supportCursor.execute(sql, (lastImportTimestamp,))                                            
        recordsModified = self.supportCursor.fetchone()[0]
        appLogger.debug("|     {0} (deleted)".format(recordsModified))
        affectedRecordCount = affectedRecordCount + recordsModified           

        appLogger.debug("| affectedRecordCount  : {0} (total)".format(affectedRecordCount))        
        self.queue.setScanResults(taskId, affectedRecordCount)
        
        lineCount = 0
        successCount = 0
        exceptionCount=0
        errorCount = 0
        warningCount = 0
        noticeCount = 0
        ignoredCount = 0        
    
    
        # Fire off the deletes
        # ====================
        appLogger.debug("|")
        appLogger.debug("| PROCESSING:")        
        appLogger.debug("|")
        appLogger.debug("|   DELETES")                        
             

        appLogger.debug("|     {0}".format(thisRecord.table))
        sql = "select id from history.import_%s_deletes" %(thisRecord.table)
        if lastImportTimestamp is None:                
            params=None
        else:
            sql = sql + " where deleted > %s"
            params=(lastImportTimestamp,)                    

        deleteDml = "delete from editable.%s" %(thisRecord.table)
        deleteDml = deleteDml + " where id = %s"

        loopCursor = loopConnection.makeCursor("loopCursor", True, True)
        loopCursor.execute(sql, params)
        
        for data in loopCursor:
            
            if lineCount % 1000 == 0:                
                self.queue.setTaskProgress(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)
            lineCount = lineCount + 1
            if lineCount % commitThreshold == 0:
                appLogger.debug("| << Transaction size threshold reached ({0}): COMMIT >>".format(lineCount))
                dataConnection.connection.commit()
            
            # Decision call to go here                                
            deleteAllowed = True
            if deleteAllowed:
                successCount = successCount + 1
                dataCursor.execute(deleteDml, (data[0],))
            else:
                warningCount = warningCount + 1
                
        loopCursor.connection.commit()                
    
        # Fire off the inserts/updates
        # ============================       
        appLogger.debug("|")
        appLogger.debug("|   INSERT/UPDATE")
                
        placeholder = "%s,%s,%s,%s"    
        for thisField in thisRecord.fields:
            if thisField.column is not None:
                placeholder = placeholder + ",%s"
        for thisField in thisRecord.additionalFields:
            placeholder = placeholder + ",%s"
            
        
        appLogger.debug("|     {0}".format(thisRecord.table))


        # OPTIMISE:
        # Is there any data for this record in editable?
        # If not, then don't bother with the costly merge view.
        sql = "select exists (select 1 from editable.{0} limit 1)".format(thisRecord.table)
        self.supportCursor.execute(sql)
        dataExists = self.supportCursor.fetchone() 
        dataExists = dataExists[0]
        appLogger.debug("|       dataExists: {0}".format(dataExists))

        # Build SQL statement to find
        # all affected records                
                    
        columnList=[]
        columnList.append("id")
        
        if dataExists:
            columnList.append("editable_record_exists")
            importSliceStart = 2
        else:
            importSliceStart = 1
        
        importSliceEnd = importSliceStart -1
        
        for thisField in thisRecord.fields:
            if thisField.column is not None:
                columnList.append(thisField.column)
                importSliceEnd = importSliceEnd +1
        for thisField in thisRecord.additionalFields:
            columnList.append(thisField.column)
            importSliceEnd = importSliceEnd +1            
        columnList.append("created")
        columnList.append("modified")

        if dataExists:    
            for thisField in thisRecord.fields:
                if thisField.column is not None:
                    columnList.append("e_%s" %(thisField.column))            
            for thisField in thisRecord.additionalFields:
                columnList.append("e_%s" %(thisField.column))
            columnList.append("e_visibility")
            columnList.append("e_security")
        
        originalEnd = len(columnList)-1
        
        if dataExists:
            source="shared.{0}_to_merge_into_editable".format(thisRecord.table)
        else:
            source="import.{0}".format(thisRecord.table)
                                                        
        sql = "select {0} from {1}".format(",".join(columnList),source)            
        
        if lastImportTimestamp is None:                
            params=None
        else:
            sql = sql + " where modified > %s::timestamp"
            params=(lastImportTimestamp,)


        # BUILD DML Statements 
        placeholder = "%s,%s,%s,%s"    
        for thisField in thisRecord.fields:
            if thisField.column is not None:
                placeholder = placeholder + ",%s"
        for thisField in thisRecord.additionalFields:
            placeholder = placeholder + ",%s"            
        insertDml = "select * from editable.%s_insert(%s)" %(thisRecord.table, placeholder)
        updateDml = "select * from editable.%s_update(%s)" %(thisRecord.table, placeholder)        

                    
        # Grab transformer function           
        moduleFilename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications", specification.name, "resources", "py", "transformation","editable"), "%s_editable_transformer.py" %(thisRecord.table))            
        module = imp.load_source("%s_editable_transformer.py" %(thisRecord.table), moduleFilename)
        transformFunction = module.transformSuppliedValues              

        # Loop through all inserted/updated records
        appLogger.debug("|       loopSql   : {0}".format(sql))
        appLogger.debug("|       insertDml : {0}".format(insertDml))
        appLogger.debug("|       updateDml : {0}".format(updateDml))

        loopCursor = loopConnection.makeCursor("loopCursor", True, True)            
        loopCursor.execute(sql, params)
    
        for data in loopCursor:
            if lineCount % 1000 == 0:                
                self.queue.setTaskProgress(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)
            lineCount = lineCount + 1
            if lineCount % commitThreshold == 0:
                appLogger.debug("| << Transaction size threshold reached ({0}): COMMIT >>".format(lineCount))
                dataConnection.connection.commit()
                            
            # Transform values
            transformedValues = transformFunction(dataCursor, data)              
            
            # Assemble values to apply
            applyValues=[data[0],"import"]            
            applyValues.extend(data[importSliceStart:importSliceEnd+1])
            applyValues.extend(transformedValues[originalEnd+1:])            

            
            if dataExists:                  
                if data["editable_record_exists"]:
                    dataCursor.execute(updateDml, applyValues)  
                    messages = dataCursor.fetchall()                                                                  
                else:
                    dataCursor.execute(insertDml, applyValues)
                    messages = dataCursor.fetchall()
            else:
                dataCursor.execute(insertDml, applyValues)
                messages = dataCursor.fetchall()
            
            success=True
            for thisMessage in messages:
                msgLevel = thisMessage[0]
                msgCode = thisMessage[1]
                msgTitle = thisMessage[2]
                msgAffectedColumns = thisMessage[3]
                msgAffectedRowCount = thisMessage[4]
                msgContent = thisMessage[5]

                self.queue.addTaskMessage(taskId, thisRecord.table, lineCount, msgLevel, msgCode, msgTitle, msgAffectedColumns, msgAffectedRowCount, "{0}: {1}".format(msgContent,transformedValues))

                if msgLevel=="warning":
                    warningCount += 1
                    success=False
                elif msgLevel=="error":
                    errorCount += 1
                    success=False
                elif msgLevel=="exception":
                    exceptionCount += 1
                    success=False     
                elif msgLevel=="notice":
                    noticeCount += 1
            
            if success:                       
                successCount = successCount + 1
        
        loopCursor.close()
    
        return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )
    
    
    def processImportSyncDeletes(self, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, specification, args):
    
        minTaskId = args["minTaskId"]
        maxTaskId = args["maxTaskId"]       
        
        successCount = 0
        exceptionCount=0
        errorCount=0
        warningCount=0
        noticeCount=0
        ignoredCount=0      
    
        self.queue.startTask(taskId, True)
        if minTaskId is not None and maxTaskId is not None:
            
            # Scanning
            for thisRecord in specification.records:
                if thisRecord.table == args["table"]:
                    
                    if minTaskId==maxTaskId:
                        sql = "select count(*) from import.%s" %(thisRecord.table)
                        sql = sql + " where last_affirmed_task_id != %s"
                        settings.appLogger.debug("|   staleSql:{0}".format(sql))
                        dataCursor.execute(sql, (minTaskId,))                    
                    else:
                        sql = "select count(*) from import.%s" %(thisRecord.table)
                        sql = sql + " where last_affirmed_task_id not (between %s and %s)"
                        settings.appLogger.debug("|   staleSql:{0}".format(sql))
                        dataCursor.execute(sql, (minTaskId,maxTaskId))                    
    
                    staleCount = int(dataCursor.fetchone()[0])
            
                    self.queue.setScanResults(taskId, staleCount)   
                    settings.appLogger.debug("|   staleCount: {0}".format(staleCount)) 
                                           
                    params=[]
                    for i in range(0,len(thisRecord.primaryKeyColumns)+1):
                        params.append("%s")
                    dml = "select * from import.%s_delete(%s)" %(thisRecord.table, cs.delimitedStringList(params,","))
                    
                    if minTaskId==maxTaskId:
                        sql = "select %s from import.%s" %(cs.delimitedStringList(thisRecord.primaryKeyColumns,","), thisRecord.table)
                        sql = sql + " where last_affirmed_task_id != %s"
                        settings.appLogger.debug("|   dataSQL: {0}".format(sql%(minTaskId)))
                        dataCursor.execute(sql, (minTaskId,))                    
                    else:
                        sql = "select %s from import.%s" %(cs.delimitedStringList(thisRecord.primaryKeyColumns,","), thisRecord.table)
                        sql = sql + " where last_affirmed_task_id not (between %s and %s)"
                        settings.appLogger.debug("|   dataSQL:{0}".format(sql%(minTaskId,maxTaskId)))
                        dataCursor.execute(sql, (minTaskId,maxTaskId))                    
                    settings.appLogger.debug("|   dml: {0}".format(dml))
                    results = dataCursor.fetchall()
                    deletedRowCount = 0
                    for data in results:
                        deleteParams=[]
                        deleteParams.append(taskId)
                        i=0
                        for thisPkColumn in thisRecord.primaryKeyColumns:
                            deleteParams.append(data[i])
                            i=i+1

                        if deletedRowCount < 10:
                            settings.appLogger.debug("|   {0}".format(deleteParams))
                        
                        dataCursor.execute(dml, tuple(deleteParams))
                        deletedRowCount += 1
                        
                        messages = dataCursor.fetchall()                        
                        for thisMessage in messages:
                            msgLevel = thisMessage[0]
                            msgCode = thisMessage[1]
                            msgTitle = thisMessage[2]
                            msgAffectedColumns = thisMessage[3]
                            msgAffectedRowCount = thisMessage[4]
                            msgContent = thisMessage[5]
                            self.queue.addTaskMessage(taskId, thisRecord.table, 0, msgLevel, msgCode, msgTitle, msgAffectedColumns, msgAffectedRowCount, msgContent)
                    
                    settings.appLogger.debug("|   deletedRowCount: {0}".format(deletedRowCount))

        self.supportConnection.connection.commit()  
          
        return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )
    
    
    def processSendToImport(self, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, specification, args):
    
        def getAction(sendMode, identification):
            if sendMode=="full":
                action="insert"                
            elif sendMode=="change":
                action=identification
            elif sendMode=="sync":
                action="merge"
            return(action)
        
        def getSendMode(importMode, fileIntent, hasData, appLogger):
            # Settle on what it is we're doing
            #
            # importMode - auto
            #            - full
            #            - change
            #            - sync
            #    
            # fileIntent - undefined
            #            - full
            #            - change
            #            - mixed
            #
            # 
            
            if importMode=="auto":
                if fileIntent=="undefined":
                    if hasData:            
                        mode = "sync"
                    else:
                        mode = "full"
                elif fileIntent=="full":
                    mode = "full"
                elif fileIntent=="change":
                    mode = "change"
                elif fileIntent=="mixed":
                    print("Imports of mixed file intents not supported")
                    raise
                
            elif importMode=="full":
                mode = "full"
                
            elif importMode=="change":
                mode = "change"
                
            elif importMode=="sync":
                mode = "sync"
                
            appLogger.debug("|  {0} (importMode={1} fileIntent={2} hasData={3})".format(mode, importMode, fileIntent, hasData))
            
            return(mode)
            
        
        appLogger = settings.appLogger
        commitThreshold = int(settings.env["dataCommitThreshold"])            
        table = args["table"]        
        importMode = args["importMode"]        
        fileIntent = args["fileIntent"]
        strategy = args["strategy"]                 
        hasData = args["hasData"]
        sendMode = getSendMode(importMode, fileIntent, hasData, appLogger)
            
        self.queue.startTask(taskId, True)        
        sql = "select count(*) from stage.{0}".format(table)
        self.supportCursor.execute(sql)
        scanCount = self.supportCursor.fetchone()[0]
        self.queue.setScanResults(taskId, scanCount)

        appLogger.debug("|  Scan count = {0}".format(scanCount))

        lineCount=0
        successCount = 0
        exceptionCount=0
        errorCount=0
        warningCount=0
        noticeCount=0
        ignoredCount=0

        # Grab record                            
        for r in specification.records:
            if r.table == table:
                record = r

        appLogger.debug("|")
        appLogger.debug("|  {0}".format(table))

        # BUILD DML STATEMENTS FOR THIS RECORD
        # ------------------------------------
        selectColumns=[]
        insertPlaceholder="select * from import.{0}_insert(".format(table)
        insertPlaceholder += "%s,%s"
        if not record.editable:
            insertPlaceholder += ",%s,%s"    

        updatePlaceholder="select * from import.{0}_update(".format(table)
        updatePlaceholder += "%s"
        if not record.editable:
            updatePlaceholder += ",%s,%s"    

        mergePlaceholder="select * from import.{0}_merge(".format(table)
        mergePlaceholder += "%s,%s"
        if not record.editable:
            mergePlaceholder += ",%s,%s"

        if record.hasPrimaryKey():
            deletePlaceholder="select * from import.{0}_delete(%s" .format(record.table)
            for column in record.primaryKeyColumns:
                deletePlaceholder+=",%s"
            deletePlaceholder += ")"
        else:
            deletePlaceholder = None
                    
        for thisField in record.fields:
            if thisField.column is not None:
                selectColumns.append(thisField.column)
                insertPlaceholder += ",%s"
                updatePlaceholder += ",%s"
                mergePlaceholder += ",%s"

        for thisField in record.additionalFields:
            insertPlaceholder+=",%s"
            updatePlaceholder+=",%s"
            mergePlaceholder+=",%s"    

        insertPlaceholder+=")"
        updatePlaceholder+=")"
        mergePlaceholder+=")"

            
        # Grab transformer functions            
        moduleFilename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications", specification.name, "resources", "py","transformation","import"), "{0}_import_transformer.py".format(table))
        module = imp.load_source("{0}_import_transformer.py".format(record.table), moduleFilename)            
        transformer = module.transformSuppliedValues

        loopSql = "select id,task_id,{0},identification from stage.{1}".format(",".join(selectColumns),table)
        selectCount = 3 +len(selectColumns)
        
        # DEBUG:
        appLogger.debug("|   Pre-computed statements:")
        appLogger.debug("|     loopSql           : {0}".format(loopSql))
        appLogger.debug("|     insertPlaceholder : {0}".format(insertPlaceholder))
        appLogger.debug("|     updatePlaceholder : {0}".format(updatePlaceholder))
        appLogger.debug("|     mergePlaceholder  : {0}".format(mergePlaceholder))
        appLogger.debug("|     deletePlaceholder : {0}".format(deletePlaceholder))

        # Loop through all staged records
        loopCursor = loopConnection.makeCursor("loopCursor", True, True)
        loopCursor.execute(loopSql)
        
        for data in loopCursor:
            if lineCount % 1000 == 0:                
                self.queue.setTaskProgress(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)                                            
            lineCount = lineCount + 1
            if lineCount % commitThreshold == 0:
                appLogger.debug("| << Transaction size threshold reached ({0}): COMMIT >>".format(lineCount))
                dataConnection.connection.commit()

            identification = data["identification"]
            workingRow = data
            del data[selectCount-1]
                            
            workingRow = transformer(dataCursor, workingRow)
     
            action = getAction(sendMode, identification)
            
            if action=="insert":
                dataCursor.execute(insertPlaceholder, tuple(workingRow))

            elif action=="update":
                del workingRow[0]
                dataCursor.execute(updatePlaceholder, tuple(workingRow))
                                    
            elif action=="delete":
                None
#                        deleteParams=[]
#                        deleteParams.append(stagedRow[1])
#                        for thisPkColumn in pkColumnLists[data[0]]:
#                            deleteParams.append(stagedRow[thisPkColumn])
#                        sql = deletePlaceholders[data[0]]
#                        dataCursor.execute(sql, tuple(deleteParams))
#    
            elif action=="merge":
                dataCursor.execute(mergePlaceholder, tuple(workingRow))
#                    
            warningFlag = False
            errorFlag = False
            exceptionFlag=False
            messages = dataCursor.fetchall()
            success=True

            for thisMessage in messages:
                msgLevel = thisMessage[0]
                msgCode = thisMessage[1]
                msgTitle = thisMessage[2]
                msgAffectedColumns = thisMessage[3]
                msgAffectedRowCount = thisMessage[4]
                msgContent = thisMessage[5]
                
                self.queue.addTaskMessage(taskId, 
                                          record.table, 
                                          lineCount, 
                                          msgLevel, 
                                          msgCode, 
                                          msgTitle, 
                                          msgAffectedColumns, 
                                          msgAffectedRowCount, 
                                          "{0}: {1}".format(msgContent,data))

                if msgLevel=="warning":
                    warningFlag = True
                    success=False
                elif msgLevel=="error":
                    errorFlag =True
                    success=False
                elif msgLevel=="exception":
                    exceptionFlag=True
                    success=False     
                elif msgLevel=="notice":
                    noticeCount += 1
                    
            if success:                       
                successCount = successCount + 1
            else:
                if exceptionFlag:
                    exceptionCount += 1
                elif errorFlag:
                    errorCount += 1
                elif warningFlag:
                    warningCount += 1
                                                        
        loopCursor.close()
    

        return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )
    

    def processStageCsv(self, dataConnection, dataCursor, taskId, processLimit, specification, args, appLogger):  
        filename = args["filename"]
        fileIdentification = args["fileIdentification"]
        self.queue.startTask(taskId, True)
        
        stager = Stager(self.queue, self.supportConnection, self.supportCursor, dataConnection, dataCursor, taskId, specification, self.settings.paths, self.settings.env["dataCommitThreshold"], self.settings.appLogger)
        stager.stageCSV(filename, processLimit)
        return (fileIdentification, stager.successCount, stager.exceptionCount, stager.errorCount, stager.warningCount, stager.ignoredCount, stager.noticeCount)

    
    def processStageJSON(self, dataConnection, dataCursor, taskId, processLimit, specification, args):
        self.queue.startTask(taskId, True)
        
        stager = Stager(self.queue, self.supportConnection, self.supportCursor, dataConnection, dataCursor, taskId, specification, self.settings.paths, self.settings.env["dataCommitThreshold"], self.settings.appLogger)
        stager.stageJSON(args["recordIdentification"], args["record"])
        return (stager.successCount, stager.exceptionCount, stager.errorCount, stager.warningCount, stager.ignoredCount, stager.noticeCount)
    
    
    def processCallExternalLoader(self, taskId, processLimit, args):  
        self.queue.startTask(taskId, True)
        self.queue.setScanResults(taskId, 1)    
        result = chimpExternalLoader.stageUsingExternalLoader(self.queue.conn, self.queue.cursor, self.settings, taskId, processLimit, args)
        return (result)        
    
    def processCheckpoint(self, dataConnection, dataCursor, streamName, settings, taskId, processLimit, args, successCountSinceCheckpoint, exceptionCountSinceCheckpoint, errorCountSinceCheckpoint,warningCountSinceCheckpoint,ignoredCountSinceCheckpoint,noticeCountSinceCheckpoint, appLogger):
        keepQueueRunning = True
        
        checkpointType = args["checkpointType"] 
        toleranceLevel = args["toleranceLevel"] 
        checkpointBehaviour = args["checkpointBehaviour"]
        
        
        # What action are we going to perform?
    
        if checkpointBehaviour =="commit":
            action="commit"
        elif checkpointBehaviour == "rollback":
            action="rollback"
        
        elif checkpointBehaviour == "tolerate":
            
            if toleranceLevel == "none" and (exceptionCountSinceCheckpoint>0 or errorCountSinceCheckpoint>0 or warningCountSinceCheckpoint>0):
                action="rollback"
                keepQueueRunning = False
            elif toleranceLevel=="warning" and (exceptionCountSinceCheckpoint>0 or errorCountSinceCheckpoint>0):
                action="rollback"
                keepQueueRunning = False 
            elif toleranceLevel=="error" and exceptionCountSinceCheckpoint>0 :
                action="rollback"
                keepQueueRunning = False 
            elif toleranceLevel=="exception":
                action="commit"
            else:
                action="commit"
        
        appLogger.debug("        -- {0} --".format(action))
        
        # Tidy-up queue
        if keepQueueRunning:
            sql = "select shared.set_checkpoint_success(%s,%s)"
            self.supportCursor.execute(sql,(taskId, streamName))        
        
        if action=="rollback":
            dataConnection.connection.rollback()
            sql = "select shared.set_checkpoint_failure(%s)"
            self.supportCursor.execute(sql,(streamName,))  
            
            
        if action=="commit":
            dataConnection.connection.commit()
        
        self.supportConnection.connection.commit()
    
        return(keepQueueRunning)
    
    
    def processScript(self, dataConnection, dataCursor, settings, taskId, processLimit, args):
        
        appLogger = settings.appLogger
        
        exceptionCount = 0
        errorCount = 0
        warningCount = 0
        ignoredCount = 0
        noticeCount = 0    
        i = 0
        thisStatement = None
        statements = []
        
        try:            
            filename = args["filename"]            
            self.queue.startTask(taskId, True)
            prepareFile = open(filename, "r")            
            
            i = 0
            for thisLine in prepareFile:
                statement = thisLine.strip()
                if statement != "" and statement[:3] != "-- ":                
                    statements.append(statement)
                    i = i + 1
        
            prepareFile.close()
            self.queue.setScanResults(taskId, i)
            
            i = 0
            for thisStatement in statements:
                if exceptionCount==0:
                    appLogger.debug("|     {0}".format(thisStatement))
                    dataCursor.execute(thisStatement)
                    i = i + 1
                    self.queue.setTaskProgress(taskId, i, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)
      
            self.supportConnection.connection.commit()
            return( (i, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )
    
        except Exception as detail:
            exceptionCount = exceptionCount + 1
            print ("")
            print ("Exception processing script: %s" %(filename))
            print (str(detail))
            print (statements)
            dataConnection.connection.rollback()  
            self.queue.addTaskMessage(taskId, None, i, "exception", "EXP", "Exception executing line:\n%s" %(thisStatement), None, 1, str(detail))
            return( (i, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )
    
        

