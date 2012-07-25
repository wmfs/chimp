import chimpspec
import os.path
import cs

class IconGenerator():
    def __init__(self,iconGeneratorTag):
        
        
        self.name = str(cs.grabAttribute(iconGeneratorTag, "name"))
        self.fixedFilename = str(cs.grabAttribute(iconGeneratorTag, "fixedFilename"))
        self.contributingFields=[]
        self.mapping={}
        
        contributingFieldsTag=iconGeneratorTag.getElementsByTagName("contributingFields")
        
        if len(contributingFieldsTag)>0:            
            contributingFieldsTag=contributingFieldsTag[0]
            fields = contributingFieldsTag.getElementsByTagName("field")
            for thisField in fields:
                column = str(cs.grabAttribute(thisField, "column"))
                self.contributingFields.append(column)
                
        simpleMappingTag=iconGeneratorTag.getElementsByTagName("simpleMapping")        
        if len(simpleMappingTag)>0:            
            simpleMappingTag = simpleMappingTag[0]            
            self.simpleMappingSourceColumn = cs.grabAttribute(simpleMappingTag, "sourceColumn")
            
            self.simpleMappingUnmappedFilename = cs.grabAttribute(simpleMappingTag, "unmappedFilename")
            self.simpleMappingUnmappedPriority = cs.grabAttribute(simpleMappingTag, "unmappedPriority")
            if self.simpleMappingUnmappedPriority is not None:
                self.simpleMappingUnmappedPriority = int(self.simpleMappingUnmappedPriority)
            
            self.simpleMappingSourceColumnIndex = None
            mappings = simpleMappingTag.getElementsByTagName("mapping")            
            if len(mappings) >0:
                priority=10
                for thisMapping in mappings:
                    value = str(cs.grabAttribute(thisMapping, "value"))
                    filename = str(cs.grabAttribute(thisMapping, "filename"))
                    self.mapping[value]=(filename, priority)
                    priority = priority + 10
        else:
            self.simpleMappingSourceColumn = None
            self.simpleMappingSourceColumnIndex = None
            self.simpleMappingUnmappedFilename = None
            self.simpleMappingUnmappedPriority = None
            
            
    def setColumnIndex(self, index):
        self.simpleMappingSourceColumnIndex = index


class Pin:

    def getBuildScript(self, fields, additionalFields, primaryKeyColumns):    
        
        newFunctionName = "synchronize_{0}_pin".format(self.name)
        
        script =          "CREATE OR REPLACE FUNCTION pinhead.{0}(\n".format(newFunctionName)
        script = script + "  p_id bigint,\n"
            
        for thisPrimaryKeyColumn in primaryKeyColumns:
            for thisField in fields:
                if thisField.column==thisPrimaryKeyColumn:
                    script = script + "  p_%s %s,\n" %(thisPrimaryKeyColumn, thisField.columnDataType)
            for thisField in additionalFields:
                if thisField.column==thisPrimaryKeyColumn:
                    script = script + "  p_%s %s,\n" %(thisPrimaryKeyColumn, thisField.columnDataType)
                                        
        for additionalColumn in self.additionalPinColumns:
            for thisField in fields:
                if thisField.column==additionalColumn:
                    script = script + "  p_%s %s,\n" %(additionalColumn, thisField.columnDataType)
            for thisField in additionalFields:
                if thisField.column==additionalColumn:
                    script = script + "  p_%s %s,\n" %(additionalColumn, thisField.columnDataType)
        
        script = script + "  p_latest_pin_x numeric,\n"
        script = script + "  p_latest_pin_y numeric,\n"
        script = script + "  p_current_pin_x numeric,\n"
        script = script + "  p_current_pin_y numeric,\n"
                
        script = script + "  p_visibility integer,\n"
        script = script + "  p_security integer"
        
        
        for thisIcon in self.iconGenerators:
            script = script + ",\n  p_%s_filename character varying" %(thisIcon.name)
            script = script + ",\n  p_%s_priority integer" %(thisIcon.name)                        
                
        script = script + ") RETURNS void AS $$\n"
        script = script + "DECLARE\n"
        script = script + "  v_latest_pin BOOLEAN;\n"
        script = script + "  v_current_pin BOOLEAN;\n"
        script = script + "BEGIN\n\n"
        script = script + "  IF p_latest_pin_x IS NOT NULL AND p_latest_pin_y IS NOT NULL THEN\n"
        script = script + "    v_latest_pin := TRUE;\n"
        script = script + "  ELSE\n"
        script = script + "    v_latest_pin := FALSE;\n"
        script = script + "  END IF;\n\n"
        
        script = script + "  IF p_current_pin_x IS NOT NULL AND p_current_pin_y IS NOT NULL THEN\n"
        script = script + "    v_current_pin := TRUE;\n"
        script = script + "  ELSE\n"
        script = script + "    v_current_pin := FALSE;\n"
        script = script + "  END IF;\n\n"
        
        script = script + "  -- Does pin need creating?\n"
        script = script + "  IF v_latest_pin AND NOT v_current_pin THEN\n"
        script = script + "    INSERT INTO pinhead.%s(\n" %(self.name)                    
        script = script + "      id,\n"
        for thisPrimaryKeyColumn in primaryKeyColumns:
            script = script + "      %s,\n" %(thisPrimaryKeyColumn)
        
        script = script + "      x,\n"
        script = script + "      y,\n"

        for additionalColumn in self.additionalPinColumns:
            script = script + "      %s,\n" %(additionalColumn)

        for thisIcon in self.iconGenerators:
            script = script + "  %s_filename,\n" %(thisIcon.name)
            script = script + "  %s_priority,\n" %(thisIcon.name)                        

        script = script + "      visibility,\n"
        script = script + "      security)\n"
        script = script + "    VALUES (\n"
        script = script + "      p_id,\n"
        
        for thisPrimaryKeyColumn in primaryKeyColumns:
            script = script + "      p_%s,\n" %(thisPrimaryKeyColumn)
        
        script = script + "      p_latest_pin_x,\n"
        script = script + "      p_latest_pin_y,\n"

        for additionalColumn in self.additionalPinColumns:
            script = script + "      p_%s,\n" %(additionalColumn)

        for thisIcon in self.iconGenerators:
            script = script + "  p_%s_filename,\n" %(thisIcon.name)
            script = script + "  p_%s_priority,\n" %(thisIcon.name)                        

        script = script + "      p_visibility,\n" 
        script = script + "      p_security);\n"
        
        script = script + "  END IF;\n\n"
        
        script = script + "  -- Does pin need updating?\n"
        script = script + "  IF v_latest_pin AND v_current_pin THEN\n"
        script = script + "    UPDATE pinhead.%s SET\n" %(self.name)                   
        script = script + "      x = p_latest_pin_x,\n"
        script = script + "      y = p_latest_pin_y,\n"

        for additionalColumn in self.additionalPinColumns:
            script = script + "      %s = p_%s,\n" %(additionalColumn, additionalColumn)
        
        for thisIcon in self.iconGenerators:
            script = script + "  %s_filename  = p_%s_filename,\n" %(thisIcon.name, thisIcon.name)
            script = script + "  %s_priority = p_%s_priority,\n" %(thisIcon.name,thisIcon.name)                        

        script = script + "      visibility = p_visibility,\n"
        script = script + "      security = p_security\n"

        
        script = script + "     WHERE id = p_id;\n"
        
        script = script + "  END IF;\n\n"

        script = script + "  -- Does pin need deleting?\n"
        script = script + "  IF NOT v_latest_pin AND v_current_pin THEN\n"
        script = script + "    DELETE FROM pinhead.%s\n" %(self.name)
        script = script + "    WHERE id = p_id;\n"
        script = script + "  END IF;\n\n"
                            
        script = script + "END;\n"
        script = script + "$$ LANGUAGE plpgsql;  \n\n"
        
#        objectRegistry.registerFunction(newFunctionName, "pinhead")

        newFunctionName = "get_{0}_pins_in_area".format(self.name)

        script = script + "CREATE OR REPLACE FUNCTION pinhead.{0} (".format(newFunctionName)
        script = script + "p_geometry geometry) RETURNS SETOF pinhead.pin_result AS $$\n"
        script = script + "DECLARE\n"
        script = script + "  v_result pinhead.pin_result;\n"
        script = script + "  this_pin record;\n"
        script = script + "BEGIN\n"
        
        sql="select id,x,y,"
        
        if self.defaultIconColumn is not None:
            sql=sql+"%s," %(self.defaultIconColumn)

        if self.urnColumn is not None:
            sql=sql+"%s," %(self.urnColumn)

        
        sql=sql+"visibility,security "
        sql=sql+"FROM pinhead.%s " %(self.name)
        sql=sql+"WHERE ST_Within(pin, $1) "        
        
        script = script + "  FOR this_pin IN EXECUTE '%s' USING p_geometry LOOP\n" %(sql)
        script = script + "    v_result.id = this_pin.id;\n"
        
        if self.urnColumn is not None:
            script = script + "    v_result.urn = this_pin.%s;\n" %(self.urnColumn)
            
        script = script + "    v_result.pin_name = '%s';\n" %(self.name)
        script = script + "    v_result.x = this_pin.x;\n"
        script = script + "    v_result.y = this_pin.y;\n"
        
        if self.defaultIconColumn is not None:
            script = script + "    v_result.icon = this_pin.%s;\n" %(self.defaultIconColumn)
        elif self.defaultIcon is not None:
            script = script + "    v_result.icon = '%s';\n" %(self.defaultIcon)
        
        # Labeller
        if self.labellerSource is not None:
            if self.labellerSource=="search":
                source = "search.get_best_%s_%s_label_for_%s(this_pin." %(self.searchDomain, self.searchTable, self.searchLinkType)
            
                if self.searchLinkType=="urn":
                    source=source+self.urnColumn
                elif self.searchLinkType=="id":
                    source=source+"id"
                
                source=source+")"
                
                                                
            script = script + "    v_result.label = %s;\n" %(source)
        else:
            script = script + "    v_result.label = null;\n"
        
        
        
        
        script = script + "    v_result.visibility = this_pin.visibility;\n"
        script = script + "    v_result.security = this_pin.security;\n"
        script = script + "    RETURN NEXT v_result;\n"
        script = script + "  END LOOP;\n"
        script = script + "END;\n"
        script = script + "$$ LANGUAGE plpgsql;\n\n"

#        objectRegistry.registerFunction(newFunctionName, "pinhead")

        return (script)
        
    def __init__(self, tableName, pinTag, appLogger):
        
        if appLogger is not None:
            appLogger.debug("  PinRecord")

        self.name=str(cs.grabAttribute(pinTag,"name"))        
        self.xColumn=str(cs.grabAttribute(pinTag,"xColumn"))
        self.yColumn=str(cs.grabAttribute(pinTag,"yColumn"))
        self.title=str(cs.grabAttribute(pinTag,"title"))
        self.description=cs.grabAttribute(pinTag,"description")
        self.defaultIcon=cs.grabAttribute(pinTag,"defaultIcon")
        self.defaultIconColumn=cs.grabAttribute(pinTag,"defaultIconColumn")
        self.urnColumn=cs.grabAttribute(pinTag,"urnColumn")
        self.yColumn=str(cs.grabAttribute(pinTag,"yColumn"))

        # labeller
        self.labellerSource=None
        self.searchDomain=None
        self.searchLinkType=None
        self.searchTable=None
        self.labellerColumn=None
        labellerTag= pinTag.getElementsByTagName("labeller")
        if len(labellerTag)>0:
            labellerTag=labellerTag[0]  
            
            searchLabellerTag= labellerTag.getElementsByTagName("searchLabeller")
            if len(searchLabellerTag)>0:
                searchLabellerTag=searchLabellerTag[0]
                self.labellerSource="search"
                self.searchDomain=cs.grabAttribute(searchLabellerTag,"domain")
                self.searchLinkType=cs.grabAttribute(searchLabellerTag,"linkType")
                self.searchTable=cs.grabAttribute(searchLabellerTag,"table")
                if self.searchTable is None:
                    self.searchTable = tableName
                
                                                    

        if appLogger is not None:
            appLogger.debug("    name    = %s" %(self.name))
            appLogger.debug("    xColumn = %s" %(self.xColumn))
            appLogger.debug("    yColumn = %s" %(self.yColumn))
            appLogger.debug("    title = %s" %(cs.prettyNone(self.title)))
            appLogger.debug("    description = %s" %(str(cs.prettyNone(self.description))))
            appLogger.debug("    defaultIcon = %s" %(cs.prettyNone(self.defaultIcon)))
            appLogger.debug("    defaultIconColumn = %s" %(cs.prettyNone(self.defaultIconColumn)))
            appLogger.debug("    urnColumn = %s" %(cs.prettyNone(self.urnColumn)))
            appLogger.debug("    yColumn = %s" %(self.yColumn))
            appLogger.debug("")
            appLogger.debug("    labeller")
            appLogger.debug("    --------")
            appLogger.debug("      labellerSource = %s" %(cs.prettyNone(self.labellerSource)))
            appLogger.debug("      searchDomain = %s" %(cs.prettyNone(self.searchDomain)))
            appLogger.debug("      searchLinkType = %s"%(cs.prettyNone(self.searchLinkType)))
            appLogger.debug("      searchTable = %s"%(cs.prettyNone(self.searchTable)))
            appLogger.debug("")
        
        
        # additional attributes columns
        self.additionalPinColumns=[]
        aaTag= pinTag.getElementsByTagName("additionalAttributes")
        if len(aaTag)>0:
            aaTag=aaTag[0]                               
            additionalPinFields=aaTag.getElementsByTagName("column")                        
            for additionalPinField in additionalPinFields:
                c = cs.grabAttribute(additionalPinField,"name")
                self.additionalPinColumns.append(c)
                
            if appLogger is not None:         
                appLogger.debug("  additionalAttributesColumns = %s" %(str(self.additionalPinColumns)))

        # Grab icon 
        self.iconGenerators=[]
        iconsTag= pinTag.getElementsByTagName("icons")
        if len(iconsTag)>0:
            iconsTag = iconsTag[0]
            iconGenerators=iconsTag.getElementsByTagName("iconGenerator")
            
            for thisIconGenerator in iconGenerators:
                iconGenerator = IconGenerator(thisIconGenerator)
                self.iconGenerators.append(iconGenerator)
                            
        if appLogger is not None:        
            appLogger.debug("")


def processSynchronizePins(queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, specification, args):
    
    pinName = args["pinName"]    
    loopCursor = loopConnection.makeCursor("loopCursor", True, True)
    
    #Grab details about pin
    sql = "select specification_name, source_schema, source_table, last_synchronized, synchronization_enabled from pinhead.pin_registry where pin_name=%s"  
    supportCursor.execute(sql, (pinName, ))
    pinRecord = supportCursor.fetchone()    
    specificationName = pinRecord[0]    
    sourceSchema = pinRecord[1]
    sourceTable = pinRecord[2]
    lastSynchronized = pinRecord[3]
    synchronizationEnabled = pinRecord[4]
    
    if synchronizationEnabled:  
    
        # Off we go then...
        queue.startTask(taskId, True)
        # Scanning 
        if lastSynchronized is None:            
            sql = "select count(*) from %s.%s" %(sourceSchema, sourceTable)
            supportCursor.execute(sql)
        else:
            sql = "select count(*) from %s.%s where %s_pin_modified >" %(sourceSchema, sourceTable, pinName)
            sql=sql+"%s"
            supportCursor.execute(sql, (lastSynchronized,))                                            
        pinCount = supportCursor.fetchone()[0]
        queue.setScanResults(taskId, pinCount)
        for thisRecord in specification.records:            
            if thisRecord.table == sourceTable:                
                for thisPin in thisRecord.pins:
                    if thisPin.name==pinName: 

                        # Construct SQL/DML
                        # =================
                        dmlColumnCount=7
                        selectList=[]
                        paramList = []

                        selectList.append("a.id")                            
                        for thisPrimaryKeyColumn in thisRecord.primaryKeyColumns:
                            selectList.append("a."+thisPrimaryKeyColumn)
                            dmlColumnCount = dmlColumnCount + 1
                                                        
                        for additionalColumn in thisPin.additionalPinColumns:
                            selectList.append("a."+additionalColumn)
                            dmlColumnCount = dmlColumnCount + 1

                        selectList.append("a." + thisPin.xColumn)
                        selectList.append("a." + thisPin.yColumn)

                        for thisGenerator in thisPin.iconGenerators:
                            dmlColumnCount = dmlColumnCount + 2
                            if thisGenerator.simpleMappingSourceColumn is not None:
                                i=0
                                for thisColumn in selectList:
                                    if thisColumn == "a.%s" %(thisGenerator.simpleMappingSourceColumn):
                                        thisGenerator.setColumnIndex(i)
                                    i=i+1
                        
                        selectList.append("p.x" )
                        selectList.append("p.y")                        
                        selectList.append("coalesce(a.visibility,%s.get_%s_default_visibility())" %(sourceSchema,sourceTable))
                        selectList.append("coalesce(a.security,%s.get_%s_default_security())" %(sourceSchema,sourceTable))
                         
                        sql = "select " + cs.delimitedStringList(selectList, ",")
                        sql = sql + " from %s.%s AS a" %(sourceSchema, sourceTable)                                                                                 
                        sql=sql+" LEFT JOIN pinhead.%s as p ON(a.id=p.id)" %(pinName)
                        
                        if lastSynchronized is not None:
                            sql=sql+" WHERE a.%s_pin_modified > " %(pinName)
                            sql=sql+"%s"
                            loopCursor.execute(sql, (lastSynchronized,))
                        else:
                            loopCursor.execute(sql)

                        # Build DML
                        dml = "select pinhead.synchronize_%s_pin(" %(pinName)
                        for i in range(0,dmlColumnCount):
                            if i>0:
                                dml=dml+","
                            dml=dml+"%s"
                        dml=dml+")"
                        
                        # Main loop                                                
                        lineCount = 0
                        successCount = 0
                        exceptionCount=0
                        errorCount=0
                        warningCount=0
                        noticeCount=0
                        ignoredCount=0   
                        
                                               
                        for data in loopCursor:                            
                            if lineCount % 1000 == 0:                
                                queue.setTaskProgress(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)
                            lineCount = lineCount + 1                            
                            
                            workingData = list(data)
                            
                            for thisGenerator in thisPin.iconGenerators:             
                                
                                if thisGenerator.simpleMappingSourceColumn is not None:
                                                       
                                    sourceColumnData = data[thisGenerator.simpleMappingSourceColumnIndex]                                
                                    
                                    if str(sourceColumnData) in thisGenerator.mapping:
                                        # Value can be mapped to a filename/priority
                                        mappedValues = thisGenerator.mapping[str(sourceColumnData)]
                                        workingData.append(mappedValues[0])
                                        workingData.append(mappedValues[1])
                                    else:
                                        # Not in mapping list, but has defaults so...
                                        if thisGenerator.simpleMappingUnmappedFilename is not None and thisGenerator.simpleMappingUnmappedPriority is not None:
                                            workingData.append(thisGenerator.simpleMappingUnmappedFilename)
                                            workingData.append(thisGenerator.simpleMappingUnmappedPriority)
                                        else:
                                            # Not mapped, no defaults... clear it off.
                                            workingData.append(None)
                                            workingData.append(None)                                            
                                else:                                    
                                    workingData.append(thisGenerator.fixedFilename)
                                    workingData.append(None)                           

                            
                            try:
                                # Apply DML statement
                                dataCursor.execute(dml, tuple(workingData))                            
                                successCount = successCount + 1
                                
                            except Exception as detail:
                                try:
                                    exceptionCount = exceptionCount + 1
#                                    dataConnection.connection.rollback()                                    
                                    queue.addTaskMessage(taskId, "%s.%s" %(sourceSchema,sourceTable), lineCount, "exception", "EXP", "Exception attempting synchronization of %s pin" %(pinName), None, 1, "Message:\n%s\n\nData:\n%s"  %(str(detail), str(data)))

                                except Exception as subDetail:
                                    print ("Error")
                                    print (detail)
                                    print ("Failed to log error:")
                                    print (subDetail)
                                    raise

                        #chimpqueue.finishTask(supportConnection, supportCursor, taskId, True, True, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)                    
                                                                                
    loopCursor.close()
    supportConnection.connection.commit()

    return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )


#    def resetPins():
#        sql = "select pinhead.reset_pins()"
#        db.data.execute(sql)
#        db.dataCommit()
#


#
#def processQueue(settings, resourceRoot, indexStrategy, db, appLogger):
#
#    def dropIndexes(pinName):    
#        appLogger.debug("  Dropping indexes")
#        filename="drop_pinhead_%s_indexes.sql" %(pinName)
#        filename= os.path.join(resourceRoot,"specifications", specification.name, "database scripts", "index scripts", filename)
#        appLogger.debug("    Script: %s" %(filename))
#        dropFile=open(filename,"r")
#        for line in dropFile:
#            stripped = line.strip("\n")
#            appLogger.debug("      %s..." %(stripped))
#            db.data.execute(stripped)
#        dropFile.close()
#        
#        
#
#    def createIndexes(pinName):    
#        appLogger.debug("  Create indexes")
#        filename="create_pinhead_%s_indexes.sql" %(pinName)
#        filename= os.path.join(resourceRoot,"specifications", specification.name, "database scripts", "index scripts", filename)
#        appLogger.debug("    Script: %s" %(filename))
#        createFile=open(filename,"r")
#        for line in createFile:
#            stripped = line.strip("\n")
#            appLogger.debug("      %s..." %(stripped))
#            db.data.execute(stripped)
#                
# 
#    def resetPins():
#        sql = "select pinhead.reset_pins()"
#        db.data.execute(sql)
#        db.dataCommit()
#               
#    def registerSyncStart(pinName):
#        sql = "select pinhead.register_sync_start(%s)"
#        db.data.execute(sql, (pinName,))
#        db.dataCommit()
#        
#    def registerSyncEstimation(pinName, estimatedRowCount):
#        sql = "select pinhead.register_sync_estimation(%s,%s)"
#        db.data.execute(sql, (pinName, estimatedRowCount))
#        startTime=db.data.fetchone()[0]
#        db.dataCommit()
#        return(startTime)
#        
#    def registerSyncProgress(pinName, progressCount):
#        sql = "select pinhead.register_sync_progress(%s,%s)"
#        db.data.execute(sql, (pinName, progressCount))
#        db.dataCommit()
#        
#    def registerSyncFinish(pinName, startTime, progressCount):
#        sql = "select pinhead.register_sync_finish(%s,%s,%s)"
#        db.data.execute(sql, (pinName, startTime, progressCount))
#        db.dataCommit()
#            
#    appLogger.debug("")
#    appLogger.debug("PINHEAD")
#    appLogger.debug("-------")
#    
#    appLogger.debug("indexStrategy               : %s" %(indexStrategy))
#    appLogger.debug("pinheadIndexRebuildThreshold: %s" %(settings.pinheadIndexRebuildThreshold))
#    
#    resetPins()
#    sql = "select a.pin_name, a.specification_name, a.table_name, a.status, a.synchronize_function, a.last_synchronized from pinhead.pin_registry as a where synchronization_enabled"
#        
#    db.support.execute(sql)
#    pins=db.support.fetchall()
#    
#    for thisPin in pins:
#        
#        pinName = thisPin[0]
#        specificationName = thisPin[1]
#        tableName = thisPin[2]
#        status = thisPin[3]
#        functionName = thisPin[4]
#        lastSynchronized= thisPin[5]
#        
#        appLogger.debug("")
#        appLogger.debug("Processing %s" %(pinName))
#        appLogger.debug("  specificationName = %s" %(specificationName))
#        appLogger.debug("  tableName         = %s" %(tableName))
#        appLogger.debug("  status            = %s" %(status))
#        appLogger.debug("  functionName      = %s" %(functionName))
#        appLogger.debug("  lastSynchronized   = %s" %(str(lastSynchronized)))
#                    
#        registerSyncStart(pinName)
#        
#        sql="select count(*) from store.%s where w_%s_pin_modified >" %(tableName, pinName)
#        sql=sql+"%s"
#
#        db.support.execute(sql, (lastSynchronized,))
#        rowCount=db.support.fetchone()[0]
#
#        appLogger.debug("  estimatedCount    = %s" %(str(rowCount)))
#        startTime = registerSyncEstimation(pinName, rowCount)
#                                        
#                
#                
#                for thisGenerator in iconGenerators:
#                    i=0
#                    for thisParam in paramList:
#                        if thisParam == thisGenerator.simpleMappingSourceColumn:
#                            thisGenerator.setColumnIndex(i)
#                        i=i+1
#                    
#                db.createNamedCursor()            
#                db.namedCursor.execute(sql, (lastSynchronized,))
#                
#                sql = "select %s(" %(functionName)
#                for i in range(0,columnCount):
#                    if i>0:
#                        sql=sql+","
#                    sql=sql+"%s"
#                sql=sql+")"
#        
#                # Decide if to drop/recreate indexes
#                if indexStrategy=="recreate" or (indexStrategy=="threshold" and rowCount > settings.pinheadIndexRebuildThreshold):
#                    dropIndexes(pinName)
#                    needToRecreateIndexes=True
#                else:
#                    appLogger.debug("  Retaining indexes")
#                    needToRecreateIndexes=False
#                    
#                    
#                progressCount=0
#                appLogger.debug("  Starting named cursor...")
#        
#                              
#                for data in db.namedCursor:
#                    workingData = data
#                    
#                    progressCount=progressCount+1
#                    if progressCount%settings.pinheadProgressReportFrequency==0:
#                        registerSyncProgress(pinName, progressCount)   
#        
#                    for thisGenerator in iconGenerators:
#                        
#                        sourceColumnData = data[thisGenerator.simpleMappingSourceColumnIndex]
#                        
#                        mappedValues = thisGenerator.mapping[str(sourceColumnData)]
#                        workingData.append(mappedValues[0])
#                        workingData.append(mappedValues[1])
#                                                
#                    db.data.execute(sql, tuple(data))
#        
#                db.closeNamedCursor()
#                appLogger.debug("  Finished named cursor.")
#                
#                db.dataCommit()
#                
#                if needToRecreateIndexes:
#                    createIndexes(pinName)
#                    db.dataCommit()
#                                
#                registerSyncFinish(pinName, startTime, progressCount)
#        
#    return(False, False)
#
## =====================================================================
#
#
