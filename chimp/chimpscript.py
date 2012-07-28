import cs
import datetime
import os.path
#import search
from chimpsql import DBObjectRegistry
from chimpsql import SpecificationSQLBuilder
from codegen import SpecificationScriptBuilder
from calc.solr import SolrFields 
import shutil

def makePostgreSQLBuildScript(settings, version):

    appLogger = settings.appLogger
    specification = settings.specification
    srid = int(settings.env["srid"])
    zones = settings.zones
    defaultZoneId = int(settings.env["defaultZoneId"])

    objectRegistry = DBObjectRegistry()
    sqlBuilder = SpecificationSQLBuilder(specification)
    scriptBuilder = SpecificationScriptBuilder(specification)


    def generateComputedDataObjects(schemaName, sourceType, sourceName, computedData, fields, paths, defaultVisibility, defaultSecurity):

        # Grab a list of distinct columns that would require 
        # the "custom" data of a MV row to be recalculated...
        solrFields = None
        customTriggeringColumns=[]        
        for element in computedData.elements:
            if element.type == "customColumn":
                customTriggeringColumns.extend(element.getTriggeringColumns())
        customTriggeringColumns = list(set(customTriggeringColumns))

        firstComputedObject = True
        customSeq=0                    
        for element in computedData.elements:

            if firstComputedObject:
                firstComputedObject = False
                file.write("\n-- Computed objects\n")
                file.write("-- ================\n\n")


            if element.type=="zoneColumn":
                zoneFunction = element.getComputedZoneFunction(sourceName, schemaName, zones, srid, defaultZoneId)
                registerAndWrite(zoneFunction, objectRegistry, file)
                registerAndWrite(element.getComputedInsertZoneTrigger(sourceName, schemaName, sourceName, zoneFunction), objectRegistry, file)                            
                registerAndWrite(element.getComputedUpdateZoneTrigger(sourceName, schemaName, sourceName, zoneFunction), objectRegistry, file)                            
      
            elif element.type=="timestampColumn":                
                timestampFunction = element.getComputedTimestampFunction(sourceName, schemaName)
                registerAndWrite(timestampFunction, objectRegistry, file)
                registerAndWrite(element.getComputedTimestampTrigger(sourceName, schemaName, sourceName, timestampFunction), objectRegistry, file)                                                        
                registerAndWrite(element.getComputedTimestampIndex(sourceName, schemaName, sourceName), objectRegistry, file)
            
            elif element.type=="ctree":
                registerAndWrite(element.getCtreeRegistrationDML(specification.name, schemaName, sourceName), objectRegistry, file)
                ctreeTable = element.getCtreeTable(sourceName)
                registerAndWrite(ctreeTable, objectRegistry, file)
                #registerAndWrite(element.getAncestorConstraintDML(ctreeTable, schemaName, sourceName), objectRegistry, file)
                #registerAndWrite(element.getDescendantConstraintDML(ctreeTable, schemaName, sourceName), objectRegistry, file)
                registerAndWrite(element.getAncestorIndex(ctreeTable), objectRegistry, file)
                registerAndWrite(element.getDescendantIndex(ctreeTable), objectRegistry, file)
                registerAndWrite(element.getDepthIndex(ctreeTable), objectRegistry, file)
                registerAndWrite(element.getDisconnectFunction(ctreeTable, sourceName), objectRegistry, file)
                registerAndWrite(element.getApplyEdgeFunction(ctreeTable, sourceName), objectRegistry, file)
                registerAndWrite(element.getAllEdgesFunction(ctreeTable, schemaName,sourceName), objectRegistry, file)
                insertFunction = element.getInsertFunction(ctreeTable, schemaName,sourceName)
                registerAndWrite(insertFunction, objectRegistry, file)
                registerAndWrite(element.getInsertTrigger(schemaName,sourceName,insertFunction), objectRegistry, file)
                deleteFunction = element.getDeleteFunction(ctreeTable, schemaName,sourceName)
                registerAndWrite(deleteFunction, objectRegistry, file)
                registerAndWrite(element.getDeleteTrigger(schemaName,sourceName,deleteFunction), objectRegistry, file)
                updateFunction = element.getUpdateFunction(ctreeTable, schemaName,sourceName)
                registerAndWrite(updateFunction, objectRegistry, file)
                registerAndWrite(element.getUpdateTrigger(schemaName,sourceName,updateFunction), objectRegistry, file)
                ctreeType = element.getType(schemaName,sourceName)
                registerAndWrite(ctreeType, objectRegistry, file)
                registerAndWrite(element.getInfoFunction(ctreeTable, schemaName,sourceName), objectRegistry, file)
                
                queueTable = element.getCtreeQueueTable(sourceName)
                registerAndWrite(queueTable, objectRegistry, file)
                triggerFunction = element.getCtreeQueueFunction(sourceName)  
                registerAndWrite(triggerFunction, objectRegistry, file)
                registerAndWrite(element.getCtreeQueueInsertTrigger(sourceName,triggerFunction), objectRegistry, file)
                registerAndWrite(element.getCtreeQueueDeleteTrigger(sourceName,triggerFunction), objectRegistry, file)
                                                
                element.generateEnableAndRecreateScript(settings.paths["repository"], specification.name, ctreeTable, schemaName,sourceName)
                element.generateDisableScript(settings.paths["repository"], specification.name, ctreeTable, schemaName,sourceName)
                


            elif element.type == "customColumn":
                customSeq += 1
                registerAndWrite(element.getCustomRegistrationDML(specification.name, schemaName, sourceName, customSeq, settings.paths["repository"]), objectRegistry, file)
                registerAndWrite(element.getCustomQueueTable(sourceName), objectRegistry, file)
                queueFunction = element.getCustomQueueFunction(sourceName)
                registerAndWrite(queueFunction, objectRegistry, file)
                registerAndWrite(element.getCustomQueueInsertTrigger(schemaName, sourceName, queueFunction),objectRegistry, file)
                registerAndWrite(element.getCustomQueueUpdateTrigger(schemaName, sourceName, queueFunction,customTriggeringColumns),objectRegistry, file)
                                     
            elif element.type=="pin":                
                registerAndWrite(element.getPinheadPinRegistrationDML(schemaName, specification.name, sourceName, settings.paths["repository"]), objectRegistry, file)
                registerAndWrite(element.getPinIdSequence(), objectRegistry, file)                            
                pinheadTable = element.getPinheadTable()
                registerAndWrite(pinheadTable, objectRegistry, file)
                registerAndWrite(element.getPinheadExistsFunction(pinheadTable), objectRegistry, file)
                registerAndWrite(element.getPinheadGeometryAddDML(srid),objectRegistry, file)
                registerAndWrite(element.getPinheadSpatialIndex(),objectRegistry, file)
                registerAndWrite(element.getPinheadSourceIdIndex(),objectRegistry, file)
                
                for index in element.additionalIndexes:
                    registerAndWrite(element.getAdditionalPinIndex(index, pinheadTable), objectRegistry, file)
                
                                
                registerAndWrite(element.getPinheadQueueTable(),objectRegistry, file)                            
                pinMaintainer = element.getPinheadPinMaintainerFunction(srid)
                registerAndWrite(pinMaintainer,objectRegistry, file)
                registerAndWrite(element.getPinheadMaintainerInsertTrigger(pinMaintainer),objectRegistry, file)
                registerAndWrite(element.getPinheadMaintainerUpdateTrigger(pinMaintainer),objectRegistry, file)
                
#                registerAndWrite(element.getPinheadQueueTableIndex(),objectRegistry, file)                
#                registerAndWrite(element.getPinheadQueueView(schemaName,sourceName),objectRegistry, file)                
                pinheadQueueFunction = element.getPinheadQueueFunction("insert",["new"])                
                registerAndWrite(pinheadQueueFunction,objectRegistry, file)
                registerAndWrite(element.getPinheadQueueInsertTrigger(schemaName, sourceName, pinheadQueueFunction),objectRegistry, file)

                pinheadQueueFunction = element.getPinheadQueueFunction("update",["old","new"])                
                registerAndWrite(pinheadQueueFunction,objectRegistry, file)
                registerAndWrite(element.getPinheadQueueUpdateTrigger(schemaName, sourceName, pinheadQueueFunction),objectRegistry, file)

                pinheadQueueFunction = element.getPinheadQueueFunction("delete",["old",])                
                registerAndWrite(pinheadQueueFunction,objectRegistry, file)            
                registerAndWrite(element.getPinheadQueueDeleteTrigger(schemaName, sourceName, pinheadQueueFunction),objectRegistry, file)
                
                if element.keyColumn is not None or element.documentType is not None or element.vicinityResultIconColumn or element.vicinityResultIconConstant is not None or element.vicinityResultLabelColumn is not None: 
                    registerAndWrite(element.getInAreaFunction(), objectRegistry, file)
                
            elif element.type=="solrDocument":
                registerAndWrite(element.getSolrDocumentRegistrationDML(schemaName, specification.name, sourceName, settings.paths["generatedSolrFormatterScriptsDir"].format(specification.name)), objectRegistry, file)
                registerAndWrite(element.getSolrDocumentQueueTable(fields), objectRegistry, file)
                solrDocumentQueueFunction = element.getSolrDocumentQueueFunction("insert",["new"])
                registerAndWrite(solrDocumentQueueFunction, objectRegistry, file)                
                registerAndWrite(element.getSolrDocumentQueueInsertTrigger(schemaName, sourceName, solrDocumentQueueFunction),objectRegistry, file)
                solrDocumentQueueFunction = element.getSolrDocumentQueueFunction("update",["old","new"])
                registerAndWrite(solrDocumentQueueFunction, objectRegistry, file)                                
                registerAndWrite(element.getSolrDocumentQueueUpdateTrigger(schemaName, sourceName, solrDocumentQueueFunction),objectRegistry, file)
                solrDocumentQueueFunction = element.getSolrDocumentQueueFunction("delete",["old"])
                registerAndWrite(solrDocumentQueueFunction, objectRegistry, file)                                                
                registerAndWrite(element.getSolrDocumentQueueDeleteTrigger(schemaName, sourceName, solrDocumentQueueFunction),objectRegistry, file)
                registerAndWrite(element.getSolrQueueView(schemaName, sourceName, fields),objectRegistry, file)
                
                if solrFields is None:
                    solrFields = SolrFields(settings)
                element.buildSolrDocumentScript(paths, specification.name, solrFields, defaultVisibility, defaultSecurity, srid)
                
    def buildVcSchema():        
        for record in specification.getUsefulEditableRecords():
            vcTable = sqlBuilder.getVersionControlCheckedOutTable(record)            
            registerAndWrite(vcTable, objectRegistry, file)                   
            registerAndWrite(sqlBuilder.getVersionControlCheckedOutAuthorIndex(record, vcTable), objectRegistry, file)


    def buildPublicationObjects():
        for record in specification.getUsefulEditableRecords():
            registerAndWrite(sqlBuilder.getSharedToMergeIntoEditableView(record), objectRegistry, file)
                    

    def generateMvObjects(schemaName):
        for entity in specification.entities:
            
            unmaterializedView = sqlBuilder.getMVUnmaterialisedView(entity, schemaName)
            registerAndWrite(unmaterializedView, objectRegistry, file)
            
            registerAndWrite(sqlBuilder.getMVSequence(entity), objectRegistry, file)
            
            mvTable = sqlBuilder.getMVTable(entity, unmaterializedView)
            registerAndWrite(mvTable, objectRegistry, file)
            
            for table in entity.tables:
                registerAndWrite(sqlBuilder.getMVTableIdIndex(entity, table, mvTable), objectRegistry, file)
            
            registerAndWrite(sqlBuilder.getMVTableVisibilityIndex(entity, mvTable), objectRegistry, file)
            registerAndWrite(sqlBuilder.getMVTableSecurityIndex(entity, mvTable), objectRegistry, file)                   
                          
            for index in entity.additionalIndexes:
                registerAndWrite(sqlBuilder.getMVTableAdditionalIndexIndex(index, entity, mvTable), objectRegistry, file)
                      
            registerAndWrite(sqlBuilder.getMVDefaultVisibilityFunction(entity), objectRegistry, file)
            registerAndWrite(sqlBuilder.getMVDefaultSecurityFunction(entity), objectRegistry, file)
            
            mvRefreshRowFunction = sqlBuilder.getMVRefreshRowFunction(entity, mvTable, unmaterializedView)
            registerAndWrite(mvRefreshRowFunction, objectRegistry, file)                         
            
#            if entity.search is not None:
#                registerAndWrite(sqlBuilder.getMVSearchDomainSourceRegistrationDML(entity), objectRegistry, file)
                                       
                #TODO: search stuff             
#                sd = search.SearchDomain()
#                sd.setFromFile(settings, entity.search.searchDomain)
#                sd.buildRecordFormatterScripts(settings, specification.name, entity.name, entity.search, entity.search.searchDomain, appLogger)
#                sd.writeDomainFunctions(file, "mv", "view", entity.name,entity.search)
#                sd.debug(settings)
               
            for table in entity.tables:                                           
                mvEntityInsertTriggerFunction = sqlBuilder.getMVEntityInsertTriggerFunction(table, entity, mvTable, mvRefreshRowFunction, schemaName)
                registerAndWrite(mvEntityInsertTriggerFunction, objectRegistry, file)                                       
                registerAndWrite(sqlBuilder.getMVEntityInsertTrigger(table, entity, schemaName, mvEntityInsertTriggerFunction), objectRegistry, file)

                mvEntityUpdateTriggerFunction = sqlBuilder.getMVEntityUpdateTriggerFunction(table, entity, mvTable, mvRefreshRowFunction, schemaName)
                registerAndWrite(mvEntityUpdateTriggerFunction, objectRegistry, file)                                       
                registerAndWrite(sqlBuilder.getMVEntityUpdateTrigger(specification, table, entity, schemaName, mvEntityUpdateTriggerFunction), objectRegistry, file)

            
            enableTriggersFilename = os.path.join(settings.paths["generatedMVSQLScriptsDir"].format(specification.name), "{0}_enable.sql".format(entity.name))
            with open(enableTriggersFilename, "w") as mvFile:
                mvFile.write(sqlBuilder.getMVEnableTriggersDDL(entity, schemaName).ddl)
            
            disableTriggersFilename = os.path.join(settings.paths["generatedMVSQLScriptsDir"].format(specification.name), "{0}_disable.sql".format(entity.name))
            with open(disableTriggersFilename, "w") as mvFile:
                mvFile.write(sqlBuilder.getMVDisableTriggersDDL(entity, schemaName).ddl)
            
            recreateTriggersFilename = os.path.join(settings.paths["generatedMVSQLScriptsDir"].format(specification.name), "{0}_recreate.sql".format(entity.name))
            with open(recreateTriggersFilename, "w") as mvFile:
                mvFile.write(sqlBuilder.getMVOnRecreateTriggersDML(entity).ddl)
            
            enableAndRecreateTriggersFilename = os.path.join(settings.paths["generatedMVSQLScriptsDir"].format(specification.name), "{0}_enable_and_recreate.sql".format(entity.name))
            with open(enableAndRecreateTriggersFilename, "w") as mvFile:
                shutil.copyfileobj(open(enableTriggersFilename, "r"), mvFile)
                shutil.copyfileobj(open(recreateTriggersFilename, "r"), mvFile)

            generateComputedDataObjects("mv", "view", mvTable.name, entity.computedData, entity.getAllFields(specification,None,True, None, None), settings.paths, entity.defaultVisibility, entity.defaultSecurity)
            
            if entity.computedData.requiresFile():                      
                calcFilename = os.path.join(settings.paths["generatedPythonScriptsDir"].format(specification.name), "calculated", "{0}_calculated_data_processor.py".format(entity.name))                
                calcFile = open(calcFilename, "w")   
                sourceFields=[]
                entity.computedData.writeProcessorFile("view", "mv", entity.name, entity, calcFile)                        
                calcFile.close()


    def buildStorageSchema(schemaName):

        atLeastOneTable = False
        
        for record in specification.records:

            makeTable = record.useful and (schemaName == "import" or record.editable)
            finalDestination = record.useful and ((schemaName == "import" and not record.editable) 
                                                      or (schemaName == "editable" and record.editable)) 
                
            if makeTable:                
                atLeastOneTable = True
                
                storageTable = sqlBuilder.getStorageTable(finalDestination, record, schemaName)
                registerAndWrite(storageTable, objectRegistry, file)

                # Write change table and triggers
                # -------------------------------
                file.write("-- Change table and triggers\n")
                
                historyTable = sqlBuilder.getStorageHistoryTable(record, schemaName, storageTable)
                registerAndWrite(historyTable, objectRegistry, file)

                if record.hasPrimaryKey():
                    registerAndWrite(sqlBuilder.getStorageHistoryChangesPrimaryKeyIndex(record, schemaName, historyTable), objectRegistry, file)                

                registerAndWrite(sqlBuilder.getStorageHistoryChangesValidUntilIndex(record, schemaName, historyTable), objectRegistry, file)

                changeProcessorFunction = sqlBuilder.getStorageHistoryChangeProcessorFunction(record, schemaName, historyTable)
                registerAndWrite(changeProcessorFunction, objectRegistry, file)
                registerAndWrite(sqlBuilder.getStorageHistoryChangeProcessorTrigger(record, schemaName, storageTable, changeProcessorFunction), objectRegistry, file)

                # Write deletes table and triggers
                # -------------------------------
                
                if schemaName == "import":
                    file.write("-- Delete table and triggers\n")
                    
                    historyDeletesTable = sqlBuilder.getStorageHistoryDeletesTable(record, schemaName, storageTable)
                    registerAndWrite(historyDeletesTable, objectRegistry, file)
                                        
                    if record.hasPrimaryKey():   
                        registerAndWrite(sqlBuilder.getStorageHistoryDeletesPrimaryKeyIndex(record, schemaName, historyTable), objectRegistry, file)  
    
                    registerAndWrite(sqlBuilder.getStorageHistoryDeletesDeletedIndex(record, schemaName, historyDeletesTable), objectRegistry, file)
    
                    deleteProcessorFunction = sqlBuilder.getStorageHistoryDeleteProcessorFunction(record, schemaName, historyDeletesTable)
                    registerAndWrite(deleteProcessorFunction, objectRegistry, file)
                    registerAndWrite(sqlBuilder.getStorageHistoryDeleteProcessorTrigger(record, schemaName, storageTable, deleteProcessorFunction), objectRegistry, file)

                if finalDestination:

                    #Write defaulting functions
                    file.write("\n")
                    file.write("-- Defaulting functions:\n\n")
        
                    registerAndWrite(sqlBuilder.getStorageDefaultVisibilityFunction(record, schemaName), objectRegistry, file)
                    registerAndWrite(sqlBuilder.getStorageDefaultSecurityFunction(record, schemaName), objectRegistry, file)
        

                    # Reference data
                    # TODO: SQL in one place
#                    for field in record.fields:                    
#                        if len(field.options) > 0:
#                            if not field.noBuild:
#                                file.write(field.getOptionSetScript())
                    
                    # Write pin triggers
                    # ------------------
#                    for pin in record.pins:
#                        pinChangeProcessor = sqlBuilder.getPinChangeProcessorFunction(record, pin, schemaName)                        
#                        registerAndWrite(pinChangeProcessor, objectRegistry, file)
#                        registerAndWrite(sqlBuilder.getPinChangeProcessorTrigger(record, pin, schemaName, storageTable, pinChangeProcessor), objectRegistry, file)
#
#                        #TODO: SQL in one place
#                        script = pin.getBuildScript(record.fields, record.additionalFields, record.primaryKeyColumns)
#                        file.write(script)

                    # Write search triggers
                    # ---------------------
#                    if record.search.enabled:
#                        
#                        searchChangeProcessor = sqlBuilder.getSearchChangeProcessorFunction(record, schemaName)                                                
#                        registerAndWrite(searchChangeProcessor, objectRegistry, file)
#                        registerAndWrite(sqlBuilder.getSearchChangeProcessorTrigger(record, schemaName, storageTable, searchChangeProcessor), objectRegistry, file)

                    # Write "delete" rule
                    # -------------------
                    #registerAndWrite(sqlBuilder.getStorageLogicalDeleteRule(record, schemaName, storageTable), objectRegistry, file)

                    # Write MV triggers
                    # -----------------
#                    for entity in specification.entities:
#                        for table in entity.tables:
#                            if table.name == record.table:                                
#                                modifiedUpdateFunction = sqlBuilder.getMVModifiedUpdateTriggerFunction(table, entity, schemaName)
#                                registerAndWrite(modifiedUpdateFunction, objectRegistry, file)                                
#                                registerAndWrite(sqlBuilder.getMVModifiedUpdateTrigger(table, entity, schemaName, storageTable, modifiedUpdateFunction), objectRegistry, file)

                    # Writing "triggered action" objects
                    # ----------------------------------
#                    firstTriggeredAction = True
#                    for action in record.mvModifyingActions:
#        
#                        if  firstTriggeredAction:
#                            firstTriggeredAction = False
#                            file.write("-- Triggered actions...\n\n")                           
#
#                        updateTimestampFunction = sqlBuilder.getMVUpdateTimestampFunction(action, record, schemaName)
#                        registerAndWrite(updateTimestampFunction, objectRegistry, file)
#        
#                        # Insert                        
#                        onInsertFunction = sqlBuilder.getMVOnInsertTriggerFunction(action, record, schemaName, updateTimestampFunction)
#                        registerAndWrite(onInsertFunction, objectRegistry, file)
#                        registerAndWrite(sqlBuilder.getMVOnInsertTrigger(action, record, schemaName, storageTable, onInsertFunction), objectRegistry, file)
#        
#                        # Update
#                        onUpdateFunction = sqlBuilder.getMVOnUpdateTriggerFunction(action, record, schemaName, updateTimestampFunction)
#                        registerAndWrite(onUpdateFunction, objectRegistry, file)
#                        registerAndWrite(sqlBuilder.getMVOnUpdateTrigger(action, record, schemaName, storageTable, onUpdateFunction), objectRegistry, file)


#                    # Writing objects to maintain computed data
#                    # =========================================
#                    generateComputedDataObjects(schemaName, "table", storageTable.name, record.computedData)
                        
                    
                                        
                # Write indexes
                # -------------                
                file.write("\n\n-- Indexes\n")
                if record.hasPrimaryKey():
                    registerAndWrite(sqlBuilder.createStorageSpecificationPrimaryKeyIndex(record, schemaName, storageTable), objectRegistry, file)

                registerAndWrite(sqlBuilder.createStorageCreatedIndex(record, schemaName, storageTable), objectRegistry, file)
                registerAndWrite(sqlBuilder.createStorageModifiedIndex(record, schemaName, storageTable), objectRegistry, file)

                if schemaName == "import":
                    registerAndWrite(sqlBuilder.createStorageLastAffirmedTaskIndex(record, schemaName, storageTable), objectRegistry, file)
             
                for thisIndex in record.additionalIndexes:
                    registerAndWrite(sqlBuilder.createStorageAdditionalIndex(thisIndex, record, schemaName, storageTable), objectRegistry, file)


                if finalDestination:

                    # Writing objects to maintain computed data
                    # (needs to go after index builds due to CTREE constraints)
                    # =========================================================
                    generateComputedDataObjects(schemaName, "table", storageTable.name, record.computedData, record.getAllMappedFieldsIncludingComputed(), settings.paths, record.defaultVisibility, record.defaultSecurity)
                    
                    if record.areaGeometryColumn is not None:
                        registerAndWrite(sqlBuilder.getAreaWithinFunction(record, schemaName), objectRegistry, file)
                        
                    if record.computedData.requiresFile():                      
                        calcFilename = os.path.join(settings.paths["generatedPythonScriptsDir"].format(specification.name), "calculated", "{0}_calculated_data_processor.py".format(record.table))                
                        calcFile = open(calcFilename, "w")                        
                        record.computedData.writeProcessorFile("table", schemaName, record.table,record, calcFile)                        
                        calcFile.close()

#                    for pin in record.pins:
#                        registerAndWrite(sqlBuilder.createStoragePinModifiedIndex(pin, record, schemaName, storageTable), objectRegistry, file)

                    # Entity timestamp indexes
#                    for entity in specification.entities:                                                
#                        for table in entity.tables:
#                            if table.name == record.table:     
#                                registerAndWrite(sqlBuilder.createStorageEntityTimestampIndex(table, entity, record, schemaName, storageTable), objectRegistry, file)                                   
               
                # Build validation scripts:
                # First up the "CORE" validation
                # This will go in the "stage" schema (the if is so it only builds once)
                # =====================================================================
                
                if schemaName == "import":                
                    validationScriptFilename = os.path.join(settings.paths["generatedValidationSQLScriptsDir"].format(specification.name), "stage", "{0}_stage_validation_function.sql".format(record.table))                
                    with open(validationScriptFilename, "w") as functionFile:
                        stageValidFunction = sqlBuilder.getStageValidFunction(record)
                        registerAndWrite(stageValidFunction, objectRegistry, functionFile)
                                                                        
                    file.write("-- Incorporate validation function")
                    file.write("\n\\i '%s'\n" %(validationScriptFilename.replace("\\","/")))

                validationScriptFilename = os.path.join(settings.paths["generatedValidationSQLScriptsDir"].format(specification.name), schemaName, "{0}_import_validation_function.sql".format(record.table))
                with open(validationScriptFilename, "w") as functionFile:
                    storageValidFunction = sqlBuilder.getStorageValidFunction(record, schemaName)
                    registerAndWrite(storageValidFunction, objectRegistry, functionFile)
                                    
                file.write("-- Incorporate validation function")
                file.write("\n\\i '%s'\n\n" %(validationScriptFilename.replace("\\","/")))

                validationScriptFilename = os.path.join(settings.paths["generatedValidationSQLScriptsDir"].format(specification.name), schemaName, "{0}_import_deletable_function.sql".format(record.table))
                with open(validationScriptFilename, "w") as functionFile:
                    storageDeletableFunction = sqlBuilder.getStorageDeletableFunction(record, schemaName, storageTable)
                    registerAndWrite(storageDeletableFunction, objectRegistry, functionFile)
                    
                file.write("-- Incorporate validation function")
                file.write("\n\\i '%s'\n\n" %(validationScriptFilename.replace("\\","/")))

                # INSERT
                # ======                                                                    
                storageInsertFunction = sqlBuilder.getStorageInsertFunction(record, schemaName, storageTable, storageValidFunction)
                registerAndWrite(storageInsertFunction, objectRegistry, file)

                # UPDATE
                # ======
                storageUpdateFunction = sqlBuilder.getStorageUpdateFunction(record, schemaName, storageTable, storageValidFunction)
                registerAndWrite(storageUpdateFunction, objectRegistry, file)

                # DELETE
                # ======
                storageDeleteFunction = sqlBuilder.getStorageDeleteFunction(record, schemaName, storageTable, storageDeletableFunction)
                registerAndWrite(storageDeleteFunction, objectRegistry, file)
                
                # MERGE 
                # =====
                storageMergeFunction = sqlBuilder.getStorageMergeFunction(record, schemaName, storageTable, storageInsertFunction, storageUpdateFunction)
                registerAndWrite(storageMergeFunction, objectRegistry, file)
                
                # BUILD IMPORT TRANSFORMATION SCRIPTS
                # ===================================                                                          
                transformerScriptFilename = os.path.join(settings.paths["generatedTransformationPythonScriptsDir"].format(specification.name), schemaName, "{0}_{1}_transformer.py".format(record.table, schemaName))
                with open(transformerScriptFilename, "w") as transformerFile:
                    transformerFile.write(scriptBuilder.getStorageTransformScript(record, schemaName))        
                              
               

        if atLeastOneTable:
            
            # EXISTS FUNCTION
            # ===============
            registerAndWrite(sqlBuilder.getStorageSpecificationExistsFunction(schemaName), objectRegistry, file)

            if finalDestination:
                
                # ENTITIES
                # ========
                generateMvObjects(schemaName)
                
                    
    def buildOptionSets():
        firstOptionSet = True
        for key in sorted(specification.optionSets.keys()):
            optionSet = specification.optionSets[key]
            if firstOptionSet:
                file.write("\n-- Reference section\n")
                file.write("-- -----------------\n\n")
                firstOptionSet=False
            registerAndWrite(optionSet.getOptionSetTable(specification.name), objectRegistry, file)
            registerAndWrite(optionSet.getOptionSetIndex(specification.name), objectRegistry, file)
            installFunction =optionSet.getInstallFunction(specification.name)                     
            registerAndWrite(installFunction, objectRegistry, file)
            registerAndWrite(optionSet.getInsertDML(installFunction), objectRegistry, file)


    def buildEditableSchema():
        buildStorageSchema("editable")

    def buildImportSchema():
        buildStorageSchema("import")
        

    def buildStageSchema():
        duplicateFile = open(os.path.join(settings.paths["generatedImportSQLScriptsDir"].format(specification.name), "remove_{0}_duplicates_from_stage.sql".format(specification.name)), "w")
        prepareFile = open(os.path.join(settings.paths["generatedImportSQLScriptsDir"].format(specification.name), "prepare_{0}_stage.sql".format(specification.name)), "w")                      
        postFile = open(os.path.join(settings.paths["generatedImportSQLScriptsDir"].format(specification.name), "post_{0}_staging.sql".format(specification.name)), "w")      
        
        if specification.dedicatedStagingAreaName is not None:
            stageSchema = sqlBuilder.getStageNativeSchema()
            registerAndWrite(stageSchema, objectRegistry, file)

        stageSequencerSequence = sqlBuilder.getStageSequencerSequence()
        stageSequencerTable = sqlBuilder.getStageSequencerTable(stageSequencerSequence)        
        stageSequencerIndex = sqlBuilder.getStageSequencerIndex(stageSequencerTable)
        stageSequencerAddFunction = sqlBuilder.getStageSequencerAddFunction(stageSequencerTable)
        
        registerAndWrite(sqlBuilder.getStageSequencerTruncateDML(stageSequencerTable, stageSequencerSequence), objectRegistry, prepareFile)
        
        registerAndWrite(stageSequencerSequence, objectRegistry, file)        
        registerAndWrite(stageSequencerTable, objectRegistry, file)        
        registerAndWrite(stageSequencerIndex, objectRegistry, file)        
        registerAndWrite(stageSequencerAddFunction, objectRegistry, file)
        
        for record in specification.getUsefulRecords():

            stageSequencerAddTriggerFunction = sqlBuilder.getStageSequencerAddTriggerFunction(record, stageSequencerAddFunction)
            registerAndWrite(stageSequencerAddTriggerFunction, objectRegistry, file)

            # BUILD TRANFORM SCRIPTS
            # ======================
            transformerScriptFilename = os.path.join(settings.paths["generatedTransformationPythonScriptsDir"].format(specification.name), "stage", "{0}_import_transformer.py".format(record.table))
            with open(transformerScriptFilename, "w") as transformerFile:                
                transformerFile.write(scriptBuilder.getStageTransformScript(record))                    
                        
            stageTable = sqlBuilder.getStageTable(record)
            registerAndWrite(stageTable, objectRegistry, file)             
            
            for field in record.getGeometryFields():
                file.write(sqlBuilder.getStageGeometryAddDML(field, srid, stageTable).ddl)                        

            registerAndWrite(sqlBuilder.getStageSequencerAddTrigger(record, stageTable, stageSequencerAddTriggerFunction), objectRegistry, file)
                       
            for index in record.additionalStageIndexes:
                additionalStageIndex = sqlBuilder.getStageAdditionalStageIndex(index, record, stageTable)
                registerAndWrite(additionalStageIndex, objectRegistry, file)
                
                prepareFile.write(additionalStageIndex.getDropStatement())

            duplicateFile.write(sqlBuilder.getStageDeleteDuplicatesDML(record, stageTable).ddl)
            
            prepareFile.write(stageTable.getTruncateStatement())
            
            registerAndWrite(sqlBuilder.getStageFunction(record), objectRegistry, file)

            # Tables exist in another schema, so create views in the
            # usual stage schema to keep things simple.            
            if specification.dedicatedStagingAreaName is not None:
                stageView = sqlBuilder.getStageView(record, stageTable)
                registerAndWrite(stageView, objectRegistry, file)
                registerAndWrite(sqlBuilder.getStageViewRule(record, stageTable, stageView), objectRegistry, file)
                
                # Chance one of the "dedicated" columns clash with Chimp's id,modified etc.
                # Build a before insert trigger to redirect if specified...
                for field in record.fields:
                    if field.redirectedFromColumn is not None:
                        redirectionFunction = sqlBuilder.getRedirectionFunction(specification.dedicatedStagingAreaName, stageTable.name, record.table, field.redirectedFromColumn, field.column)
                        registerAndWrite(redirectionFunction, objectRegistry, file)
                        registerAndWrite(sqlBuilder.getRedirectionTrigger(stageTable.name,redirectionFunction), objectRegistry, file)
        
        postFile.close()        
        duplicateFile.close() 
        prepareFile.close()   
    
    def buildDropScript():
        objectRegistry.writeDropScript(settings.paths["dropSQLFile"].format(specification.name))            
                
    
    def buildIndexScripts():
        tableList = []
        for index in objectRegistry.indexes:
            if index.tableName not in tableList:
                tableList.append(index.schema + "." + index.tableName)

        for thisTable in tableList:                    
            filename = "drop_{0}_indexes.sql".format(str(thisTable).replace(".", "_"))
            filename = os.path.join(settings.paths["generatedIndexesSQLScriptsDir"].format(specification.name), filename)
            dropFile = open(filename, "w")

            filename = "create_{0}_indexes.sql".format(str(thisTable).replace(".", "_"))
            filename = os.path.join(settings.paths["generatedIndexesSQLScriptsDir"].format(specification.name), filename)
            createFile = open(filename,"w")

            for index in objectRegistry.indexes:             
                if thisTable == "{0}.{1}".format(index.schema, index.tableName):
                    dropFile.write("{0}\n".format(index.getDropStatement()))
                    createFile.write("{0}{1}\n".format("" if index.droppable else "-- ", index.ddl))

            dropFile.close();    
            createFile.close();

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


    def buildSharedSchema():

        registerAndWrite(sqlBuilder.getSharedSpecificationRegisterDML(), objectRegistry, file)

        for record in specification.getUsefulRecords():
            sharedSequence = sqlBuilder.getSharedSequence(record)                
            sharedNextIdFunction = sqlBuilder.getSharedNextIdFunction(record, sharedSequence)
            
            registerAndWrite(sharedSequence, objectRegistry, file)
            registerAndWrite(sharedNextIdFunction, objectRegistry, file)


    def buildWorkingSchema():
        
        for record in specification.getUsefulRecords():
            workingView = sqlBuilder.getWorkingView(record)
            registerAndWrite(workingView, objectRegistry, file)
            registerAndWrite(sqlBuilder.getWorkingRemoteInsertRule(record, workingView), objectRegistry, file)
            registerAndWrite(sqlBuilder.getWorkingRemoteUpdateRule(record, workingView), objectRegistry, file)
            registerAndWrite(sqlBuilder.getWorkingRemoteDeleteRule(record, workingView), objectRegistry, file)
   

                          
    file=open(settings.paths["buildSQLFile"].format(specification.name), "w")    
    appLogger.info("Vendor : PostgreSQL")
    appLogger.info("Version: "+str(version))
    

    #Write comments
    file.write("-- PostgreSQL %d table build script\n" %(version))
    file.write("-- To support import and processing of "+cs.prettyNone(specification.label)+" ("+specification.version+") data\n")
    now = datetime.datetime.now()
    file.write("-- Generated by Chimp "+now.strftime("%d-%m-%Y %H:%M:%S")+"\n")
    file.write("--\n")
    
    
        
    buildSharedSchema()
    buildOptionSets()
    buildStageSchema()
    buildImportSchema()  
    buildEditableSchema()
    buildPublicationObjects()    
    buildVcSchema()
    buildWorkingSchema()        
    buildIndexScripts()    
    buildDropScript()
    
    # Finish up
    file.close()
    

def registerAndWrite(dbObj, objectRegistry, file):
    objectRegistry.register(dbObj)    
    file.write(dbObj.ddl)

def makeBuildScript(settings):    
    makePostgreSQLBuildScript(settings, 9)    

    