'''
Created on 18 Jan 2012

@author: ryan
'''
import cs
import json
import os
import chimpExternalLoader
import fnmatch
import re
import taskqueue.queueCtree as queueCtree



class Queuer:
    '''
    classdocs
    '''


    def __init__(self, supportConnection, supportCursor, settings, queue):
        self.supportConnection = supportConnection
        self.supportCursor = supportCursor
        self.settings = settings
        self.queue = queue
        self.commitFrequency = settings.args.commitfrequency 
        self.checkpointBehaviour = settings.args.checkpointbehaviour
        self.toleranceLevel = settings.args.tolerancelevel
        self.stream = settings.args.streamname
        
        if settings.specification is not None:
            self.specificationName = settings.specification.name
        else:
            self.specificationName = None
 


    def queueCalculation(self, restriction, specificationRestriction, stream, groupId):

        appLogger = self.settings.appLogger
#        stream = self.settings.args.streamname       
#        groupId = self.settings.args.groupid
        
        
        
        
        if specificationRestriction is not None:
            specifications = specificationRestriction.split(",")
            specificationRestriction = ",".join(map(lambda s:"'{0}'".format(s),specifications))
        else:
            specificationRestriction = None            
        
        appLogger.debug("")
        appLogger.debug("Compute data")
        appLogger.debug("------------")
        if restriction is not None:
            restriction = restriction.split(",")
            processCustomColumns = True if "custom" in restriction else False
            processCtree = True if "ctree" in restriction else False
            processPins = True if "pins" in restriction else False
            processSolrDocuments = True if "solrDocuments" in restriction else False
        else:                
            processCustomColumns = True
            processCtree = True
            processPins = True
            processSolrDocuments = True
                        
        appLogger.debug("  stream                   : {0}".format(stream))
        appLogger.debug("  specificationRestriction : {0}".format(specificationRestriction))
        appLogger.debug("  Restriction:")
        appLogger.debug("    processCtree         : {0}".format(processCtree))
        appLogger.debug("    processCustomColumns : {0}".format(processCustomColumns))
        appLogger.debug("    processPins          : {0}".format(processPins))
        appLogger.debug("    processSolrDocuments : {0}".format(processSolrDocuments))

        if processCtree:
            import taskqueue.queueCtree as queueCtree
            queueCtree.queueTasks(self, self.settings, None, stream, specificationRestriction, groupId, appLogger) 

        if processCustomColumns:
            import taskqueue.queueCustom as queueCustom
            queueCustom.queueTasks(self, self.settings, stream, specificationRestriction, groupId, appLogger) 

        if processPins:
            import taskqueue.queuePins as queuePins
            queuePins.queueTasks(self, self.settings, stream, specificationRestriction, groupId, appLogger) 

        if processSolrDocuments:
            import taskqueue.queueSolrDocument as queueSolrDocument
            queueSolrDocument.queueTasks(self, self.settings, stream, specificationRestriction, groupId, appLogger) 
            
    def _queueCtreeDisable(self, settings, groupId, sourceName):        
        args = {}
        filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources", "sql","indexes"), "drop_ctree_%s_closure_indexes.sql" % (sourceName))                        
        args["filename"] = filename
        self.queue.queueTask(groupId,  self.stream, "script" , "Drop %s closure indexes" %(sourceName), None, None, None, json.dumps(args), False)            
        
        args = {}
        filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources", "sql","ctree"), "%s_disable.sql" % (sourceName))                        
        args["filename"] = filename
        self.queue.queueTask(groupId,  self.stream, "script" , "Disable %s closure tree" %(sourceName), None, None, None, json.dumps(args), False)            


    def _queueCtreeEnable(self, settings, groupId, sourceName):        
        args = {}
        filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources", "sql","ctree"), "%s_enable_and_recreate.sql" % (sourceName))                                                                
        args["filename"] = filename
        self.queue.queueTask(groupId,  self.stream, "script" , "Build %s closure tree" %(sourceName), None, None, None, json.dumps(args), False)            

        
    def queueImport(self, groupId):
    
        settings = self.settings
     
        if settings.specification.dedicatedStagingAreaName is None:
            nativeStageSchema = "stage"
        else:
            nativeStageSchema = settings.specification.dedicatedStagingAreaName 
    
    
        enableMv = False
        enableCtree = False
        
#        self.stream = settings.args.streamname
#        self.specificationName = settings.specification.name
#    
    #    supportConnection = settings.db.makeConnection("support")
    #    supportCursor = supportConnection.makeCursor("supportCursor", False, False)
    
#        self.commitFrequency = settings.args.commitfrequency 
#        self.checkpointBehaviour = settings.args.checkpointbehaviour
        self.importMode = settings.args.importmode
    
        #(supportConnection, supportCursor) = settings.db.makeConnection("support", False, False)
        self.removeDuplicates = settings.specification.autoRemoveStageDuplicates
    
        # ===============
        # [1] Queue files
        # ===============

        if settings.args.json is not None:
            (queuedTasks, minTaskId, maxTaskId) = self._queueJSON(groupId, settings.specification, self.stream, self.specificationName, settings.args.limit, settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour, settings.paths, self.removeDuplicates, self.importMode)
            fileIntent="undefined"
        elif settings.specification.sourceType=="csv":
            (queuedTasks, fileIntent, minTaskId, maxTaskId) = self._queueCsvFiles(groupId, settings.specification, self.stream, self.specificationName, settings.args.limit, settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour, settings.args.files, settings.paths, self.removeDuplicates,settings.args.recurse, settings.args.filenameregex, self.importMode)
        elif settings.specification.sourceType=="external":
            (queuedTasks, minTaskId, maxTaskId) = self._queueExternalLoaderFiles(groupId, self.stream, self.specificationName, settings.specification.externalLoaderName, nativeStageSchema, settings.specification.externalLoaderProfile, settings.specification.externalLoaderVariables, settings.args.limit,settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour, settings.args.files, settings.paths, settings.db.credentials, settings.env, self.removeDuplicates, settings.args.recurse, settings.args.filenameregex, self.importMode)
            fileIntent="full"
            
        
        args = {}
        args["specification"] = self.specificationName    
    
    # =======================
        sql = "select import.%s_exists()" %(self.specificationName)
        self.supportCursor.execute(sql)        
        hasData = self.supportCursor.fetchone()[0]
                        
        if not hasData:
        
            # ADD RECORD INDEX DROPS
            for thisRecord in settings.specification.records:
                if thisRecord.useful:
                    args = {}
                    filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications", self.specificationName, "resources", "sql", "indexes"), "drop_import_%s_indexes.sql" % (thisRecord.table))                        
                    args["filename"] = filename                               
                    self.queue.queueTask(groupId, self.stream, "script" , "Drop import.%s indexes" %(thisRecord.table), None, None, None, json.dumps(args), False)            
        
            # ADD CHECKPOINT
            self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
        
            # ADD ENTITY RECORD INDEX DROPS AND DISABLE                                    
            for thisEntity in settings.specification.entities:
                enableMv = True
        
                args = {}
                filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources","sql","indexes"), "drop_mv_%s_indexes.sql" % (thisEntity.name))                        
                args["filename"] = filename
                self.queue.queueTask(groupId,  self.stream, "script" , "Drop %s mv indexes" %(thisEntity.name), None, None, None, json.dumps(args), False)            
                
                args = {}
                filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources", "sql", "mv"), "%s_disable.sql" % (thisEntity.name))                        
                args["filename"] = filename
                self.queue.queueTask(groupId, self.stream,  "script" , "Disable %s mv" %(thisEntity.name), None, None, None,json.dumps(args), False)            
        
            # ADD CHECKPOINT
            if enableMv:            
                self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
                
            
            # ADD CTREE INDEX DROPS AND DISABLE
            for thisRecord in settings.specification.records:
                if thisRecord.useful:
                    if thisRecord.hasCtree():
                        enableCtree = True
                        self._queueCtreeDisable(settings, groupId, thisRecord.table)
            for thisEntity in settings.specification.entities:    
                if thisEntity.hasCtree():
                    enableCtree = True
                    self._queueCtreeDisable(settings, groupId, thisEntity.name)
            
            if enableCtree:            
                self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)                    
        
        # ADD SENT TO IMPORT
        
        for record in settings.specification.records:
            if record.useful:
                args = {}
                args["specification"] = self.specificationName         
                args["importMode"] = self.importMode
                args["fileIntent"] = fileIntent
                args["strategy"] = "speed"
                args["table"] = record.table
                args["hasData"]=hasData
                self.queue.queueTask(groupId,  self.stream,  "sendtoimport" , "Send '{0}' to import".format(record.table), None, None, None, json.dumps(args), False)
                self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
                self.queue.queueAVacuum(settings.args.vacuumstrategy, groupId, self.stream, "import", record.table)
                
        # If we're in sync mode then we may need to delete some things
        if self.importMode=="sync":
            for record in settings.specification.records:
                if record.useful:
                    args = {}
                    args["specification"] = self.specificationName         
                    args["importMode"] = self.importMode
                    args["fileIntent"] = fileIntent
                    args["minTaskId"] = minTaskId
                    args["maxTaskId"] = maxTaskId
                    args["table"] = record.table
                    args["hasData"]=hasData
                    self.queue.queueTask(groupId,  self.stream,  "importsyncdeletes" , "Process '{0}' sync deletes".format(record.table), None, None, None, json.dumps(args), False)
                    self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
                    self.queue.queueAVacuum(settings.args.vacuumstrategy, groupId, self.stream, "import", record.table)                
        
        committedForIndexes=False
        if not hasData:
            for thisRecord in settings.specification.records:
                if thisRecord.useful:
                    
                    if not committedForIndexes:
                        committedForIndexes = True
                        self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)

                    # ADD INDEXES
                    args = {}
                    filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources", "sql","indexes"), "create_import_%s_indexes.sql" % (thisRecord.table))                        
                    args["filename"] = filename            
                    self.queue.queueTask(groupId,  self.stream, "script" , "Create import.%s indexes" %(thisRecord.table), None, None, None, json.dumps(args), False)            
                    self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
                        
    # ================================
    
        args = None
    
        atLeastOneEditable = False
        for quickCheck in settings.specification.records:
            if quickCheck.editable:
                atLeastOneEditable = True
            
    #=================
        if atLeastOneEditable:
            sql = "select editable.%s_exists()" %(self.specificationName)
            self.supportCursor.execute(sql)        
            hasData = self.supportCursor.fetchone()[0]
        
            if not hasData:
                for thisRecord in settings.specification.records:
                    if thisRecord.useful:
                        args = {}
                        filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications", self.specificationName, "resources", "sql","indexes"), "drop_editable_%s_indexes.sql" % (thisRecord.table))                                            
                        args["filename"] = filename            
                        self.queue.queueTask(groupId,  self.stream, "script" , "Drop editable.%s indexes" %(thisRecord.table), None, None, None, json.dumps(args), False)            
            
            self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)

            firstEditable=True
            for record in settings.specification.records:
                if record.useful:
                    if firstEditable:
                        firstEditable = False
                        args = {}
                        self.queue.queueTask(groupId,  self.stream, "recordtimestamp" , "Record current timestamp", None, None, None, json.dumps(args), False)

                    args = {}
                    args["specification"] = self.specificationName
                    args["table"] = record.table
                    args["hasData"]=hasData                 
                    self.queue.queueTask(groupId,  self.stream, "sendtoeditable" , "Make '{0}' editable".format(record.table), None, None, None, json.dumps(args), False)
                    self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
                    self.queue.queueAVacuum(settings.args.vacuumstrategy, groupId, self.stream, "editable", record.table)

            args = {}
            args["specification"] = self.specificationName
            self.queue.queueTask(groupId,  self.stream, "finisheditable" , "Finish send to editable process", None, None, None, json.dumps(args), False)

                           
            if not hasData:          
                
                for thisRecord in settings.specification.records:
                    if thisRecord.useful:
                        args = {}
                        filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources", "sql","indexes"), "create_editable_%s_indexes.sql" % (thisRecord.table))                    
                        args["filename"] = filename            
                        self.queue.queueTask(groupId,  self.stream, "script" , "Create editable.%s indexes" %(thisRecord.table), None, None, None, json.dumps(args), False)            
                        self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)  
    
    #======================
        if enableCtree:
            for thisRecord in settings.specification.records:
                if thisRecord.useful:
                    if thisRecord.hasCtree():
                        self._queueCtreeEnable(settings, groupId, thisRecord.table)
            for thisEntity in settings.specification.entities:    
                if thisEntity.hasCtree():
                    self._queueCtreeEnable(settings, groupId, thisEntity.name)
                     
            self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)                    

    #======================
        for thisRecord in settings.specification.records:
            if thisRecord.useful:
                if thisRecord.hasCtree():
                    if thisRecord.editable:
                        schemaRestriction="editable"
                    else:
                        schemaRestriction="import"
                    queueCtree.queueTasks(self, settings, schemaRestriction, self.stream, "'{0}'".format(self.specificationName), groupId, settings.appLogger)
                    
    #======================
        
        if enableMv:
            self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)  
            for thisEntity in settings.specification.entities:
                args = {}            
                filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources", "sql","mv"), "%s_enable_and_recreate.sql" % (thisEntity.name))                                            
                args["filename"] = filename            
                self.queue.queueTask(groupId,  self.stream, "script" , "Enable %s mv" %(thisEntity.name), None, None, None, json.dumps(args), False)                
                self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
                
                args = {}
                filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications",self.specificationName,"resources", "sql","indexes"), "create_mv_%s_indexes.sql" % (thisEntity.name))                                            
                args["filename"] = filename
                self.queue.queueTask(groupId,  self.stream, "script" , "Create %s indexes" %(thisEntity.name), None, None, None, json.dumps(args), False)                
                self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)

            
#        if enableCtree:    
#            for thisRecord in settings.specification.records:
#                if thisRecord.useful:
#                    if thisRecord.ancestorColumn is not None or thisRecord.descendantColumn is not None:
#                        enableCtree = True
#                        args = {}
#                        filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specification files",self.specificationName,"sql","ctree"), "%s_enable_and_recreate.sql" % (thisRecord.table))                                                                
#                        args["filename"] = filename
#                        self.queue.queueTask(groupId,  self.stream, "script" , "Build %s closure tree" %(thisRecord.table), None, None, None, json.dumps(args), False)            
#    
#            self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
    
# OLD SEARCH WENT HERE                
#        if settings.args.chainsearch:
#            sql = "select domain_name,source_type,source_schema,source_name,specification_name,last_synchronized,config_location from search.active_sources where specification_name=%s"
#            self.supportCursor.execute(sql, (self.specificationName,))
#            sources = self.supportCursor.fetchall()            
#            
#            domains=[]
#            for thisSource in sources:
#                if thisSource[0] not in domains:
#                    domains.append(thisSource[0])
#    
#    
#            domainsToRebuild=[]                
#            for thisDomain in domains:
#                sql  = "select search.is_there_any_%s_data()" %(thisDomain)
#                self.supportCursor.execute(sql, (self.specificationName,))
#                hasData = self.supportCursor.fetchone()[0]
#                if not hasData:
#                    domainsToRebuild.append(thisDomain)
#                    args = {}
#                    filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("search domain files",thisDomain,"sql","indexes"), "drop_search_%s_indexes.sql" % (thisDomain))                                                                
#                    args["filename"] = filename            
#                    self.queue.queueTask(groupId,  self.stream, "script" , "Drop search.%s indexes" %(thisDomain), None, None, None, json.dumps(args), False)            
#            for thisSource in sources:
#                args = {}
#                args["domainName"] = thisSource[0]        
#                args["sourceType"] = thisSource[1]
#                args["sourceSchema"] = thisSource[2]
#                args["sourceName"] = thisSource[3]
#                args["specification"] = thisSource[4]
#                args["lastSynchronized"] = thisSource[5]
#                args["configLocation"] = thisSource[6]            
#                args["recordLimit"] = None
#                self.queue.queueTask(groupId,  self.stream, "syncSearchSource" , "Refresh %s (%s)" %(thisSource[0], thisSource[3]), None, None, None, json.dumps(args), False)
#            
#            for thisDomain in domainsToRebuild:
#                args = {}
#                filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("search domain files",thisDomain,"sql","indexes"), "create_search_%s_indexes.sql" % (thisDomain))                                                                
#                args["filename"] = filename            
#                self.queue.queueTask(groupId,  self.stream, "script" , "Create search.%s indexes" %(thisDomain), None, None, None, json.dumps(args), False)            
#        
#            self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)

                        
        # OLD PINHEAD WENT HERE
        
        # =======================================================================
        # Queue calculated data tasks for this specification
        
        
        
#        for record in settings.specification.records:
#            if record.useful:        
#                record.computedData.addTasks(settings, self, groupId, self.stream)
#        for entity in settings.specification.entities:
#            entity.computedData.addTasks(settings, self, groupId, self.stream)

            
        self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
        self.queue.queueAVacuum(settings.args.vacuumstrategy, groupId, self.stream, None, None)          
        self.queue.queueCheckpoint(groupId, self.stream, "major", settings.args.tolerancelevel, self.commitFrequency, self.checkpointBehaviour)
        self.supportCursor.connection.commit()

    def _queueCsvFiles(self, groupId, specification, stream, specificationName, lineLimit, toleranceLevel, commitFrequency, checkpointBehaviour, fileList, paths, removeDuplicates, recurse, filenameRegex, importMode):
    
        queuedTasks = False
        allFiles = self._turnArgsIntoFileList(fileList, recurse, filenameRegex)
    
        minTaskId = None
        maxTaskId = None
        
        if len(allFiles) > 0:
            
            # ADD CLEAR STAGE        
            self._queueClearStageTask(groupId, stream, specificationName, paths);            
            # ADD CHECKPOINT
            self.queue.queueCheckpoint(groupId, stream, "major", toleranceLevel, commitFrequency, checkpointBehaviour)
    
    
            # ADD INDIVIDUAL FILES        
            args = {}
            args["specification"] = specificationName
            args["importMode"] = importMode
            
            undefinedCount=0
            fullCount=0
            changeCount=0
     
            for thisFile in allFiles:  
                shortLabel = "Stage %s" % (os.path.split(thisFile)[1])                     
                args["filename"] = thisFile                               
                        
                (scanCount, identification) = self._scanStageCsv(thisFile, specification)
                if identification=="full":
                    fullCount=fullCount+1
                elif identification=="change":
                    changeCount=changeCount+1
                elif identification=="undefined":
                    undefinedCount=undefinedCount+1                    
                
                args["fileIdentification"] = identification                               
                taskId = self.queue.queueTask(groupId, stream, "stagecsv" , shortLabel, thisFile, lineLimit, scanCount, json.dumps(args), True)
                
                if minTaskId is None:
                    minTaskId = taskId
                    maxTaskId = taskId
                else:
                    if taskId > maxTaskId:
                        maxTaskId = taskId
                    
                queuedTasks = True
            
                self.queue.queueCheckpoint(groupId, stream, "major", toleranceLevel, commitFrequency, checkpointBehaviour)
   
            # ADD FINISH STAGE
            self._queueFinishStageTask(groupId, stream, specificationName, paths)
                  
            # ADD DUPLICATES
            self._queueRemoveDuplicatesTask(groupId, stream, specificationName, toleranceLevel, commitFrequency, checkpointBehaviour, removeDuplicates, paths)
    
    
            if undefinedCount >0 and fullCount == 0 and changeCount == 0:
                fileIntent = "undefined"
            elif undefinedCount ==0 and fullCount > 0 and changeCount == 0:
                fileIntent = "full"
            elif undefinedCount ==0 and fullCount == 0 and changeCount > 0:
                fileIntent = "change"
            elif undefinedCount ==0 and fullCount == 0 and changeCount == 0:
                fileIntent = "undefined"
            else:
                fileIntent = "mixed"
    
                
        return ((queuedTasks, fileIntent, minTaskId, maxTaskId))
    
    
    def _queueExternalLoaderFiles(self, groupId, stream, specificationName, externalLoaderName, schemaName, externalLoaderProfile, externalLoaderVariables, limit,toleranceLevel, commitFrequency, checkpointBehaviour, fileList, paths, dbCredentials, env, removeDuplicates, recurse, filenameRegex, importMode):
        queuedTasks=False
        minTaskId = None
        maxTaskId = None
    
        externalLoader = chimpExternalLoader.ExternalLoader(externalLoaderName, externalLoaderProfile, paths["config"])
    
        allFiles = self._turnArgsIntoFileList(fileList, recurse, filenameRegex)
        if len(allFiles) > 0:    
            # ADD CLEAR STAGE        
            self._queueClearStageTask(groupId, stream, specificationName, paths) 
            # ADD CHECKPOINT
            self.queue.queueCheckpoint(groupId, stream, "major", toleranceLevel, commitFrequency, checkpointBehaviour)
        
            # ADD INDIVIDUAL FILES
            args={}        
            args["specification"] = specificationName
            args["commandname"] = externalLoader.getCommandName()
            args["currentworkingdirectory"] = externalLoader.getCurrentWorkingDirectory()
            args["schema"] = schemaName
            args["importMode"] = importMode
            
            valuePool = dbCredentials
            valuePool.update(env)
            valuePool.update(externalLoaderVariables)
            valuePool["schema"]= schemaName
            i =0 
            okToAdd=True
            
            for thisFile in allFiles:                
                if okToAdd:    
                    
                    valuePool["fullpath"] = thisFile                                
                    valuePool["filename"] = os.path.basename(thisFile)
                    
                    f=valuePool["filename"].split(".")  
                    if len(f)==2:                   
                        valuePool["filenameWithoutExtension"] = f[0]
                    
                    args["commandargs"] = externalLoader.getFullCommand(valuePool)  
                    shortLabel = "Call %s (%s)" % (externalLoaderName, externalLoaderProfile)    
                    taskId = self.queue.queueTask(groupId, stream, "callexternalloader" , shortLabel, thisFile, limit, None, json.dumps(args), True)
                    if minTaskId is None:
                        minTaskId = taskId
                        maxTaskId = taskId
                    else:
                        if taskId > maxTaskId:
                            maxTaskId = taskId
        
                    i=i+1
                    if limit is not None:
                        if i==limit:
                            okToAdd = False
                            
            # ADD CHECKPOINT
            self.queue.queueCheckpoint(groupId, stream, "major", toleranceLevel, commitFrequency, checkpointBehaviour)                    
            # ADD FINISH STAGE
            self._queueFinishStageTask(groupId, stream, specificationName, paths)
            # ADD DUPLICATES
            self._queueRemoveDuplicatesTask(groupId, stream, specificationName, toleranceLevel, commitFrequency, checkpointBehaviour, removeDuplicates, paths)                       
                
        return ((queuedTasks, minTaskId, maxTaskId))
    
    def _queueJSON(self, groupId, specification, stream, specificationName, lineLimit, toleranceLevel, commitFrequency, checkpointBehaviour, paths, removeDuplicates, importMode):
        queuedTasks = False
        minTaskId = None
        maxTaskId = None
    
        # ADD CLEAR STAGE        
        self._queueClearStageTask(groupId, stream, specificationName, paths) 
        # ADD CHECKPOINT
        self.queue.queueCheckpoint(groupId, stream, "major", toleranceLevel, commitFrequency, checkpointBehaviour)
    
        # ADD INDIVIDUAL FILES
        args = {"specification": specificationName,
                "importMode": importMode,
                "recordIdentification": "merge",
                "record": json.loads(self.settings.args.json)}
        
        #TODO: sort out encoding and deletes
        
        taskId = self.queue.queueTask(groupId, stream, "stagejson", "Stage JSON", None, lineLimit, None, json.dumps(args), True)
              
        
        # ADD CHECKPOINT
        self.queue.queueCheckpoint(groupId, stream, "major", toleranceLevel, commitFrequency, checkpointBehaviour)                    
        # ADD FINISH STAGE
        self._queueFinishStageTask(groupId, stream, specificationName, paths)
        # ADD DUPLICATES
        self._queueRemoveDuplicatesTask(groupId, stream, specificationName, toleranceLevel, commitFrequency, checkpointBehaviour, removeDuplicates, paths)                       
                
        return ((queuedTasks, taskId, taskId))
    
    
    def _queueClearStageTask(self, groupId, stream, specificationName, paths):
        args = {}
        filename = cs.getChimpScriptFilenameToUse(paths["repository"], ("specifications",specificationName,"resources", "sql","import"), "prepare_%s_stage.sql" % (specificationName))    
        args["filename"] = filename
        self.queue.queueTask(groupId, stream, "script" , "Prepare stage", None, None, None, json.dumps(args), False)

    def _queueFinishStageTask(self, groupId, stream, specificationName, paths):
        args = {}
        filename = cs.getChimpScriptFilenameToUse(paths["repository"], ("specifications",specificationName,"resources", "sql","import"), "post_%s_staging.sql" % (specificationName))    
        args["filename"] = filename
        self.queue.queueTask(groupId, stream,  "script" , "Finish stage", None, None, None, json.dumps(args), False)

    
    def _queueRemoveDuplicatesTask(self, groupId, stream, specificationName, toleranceLevel, commitFrequency, checkpointBehaviour, removeDuplicates, paths):
        if removeDuplicates:
            self.queue.queueCheckpoint(groupId, stream, "major", toleranceLevel, commitFrequency, checkpointBehaviour)
            args = {}
            filename = cs.getChimpScriptFilenameToUse(paths["repository"], ("specifications",specificationName,"resources", "sql","import"), "remove_%s_duplicates_from_stage.sql" % (specificationName))    
            args["filename"] = filename
            self.queue.queueTask(groupId, stream,  "script" , "Remove duplicates", None, None, None, json.dumps(args), False)
            self.queue.queueCheckpoint(groupId, stream, "major", toleranceLevel, commitFrequency, checkpointBehaviour)        
        
        
        
    
    def _scanStageCsv(self, filename, specification):
        
        identification = None
        if specification.fullRegex is None and specification.changeRegex is None:
            try:
                if specification.encoding is None:        
                    with open(filename, "r") as f:
                        for scanCount, l in enumerate(f):
                            pass
                else:
                    with open(filename, "r", encoding="%s"%(specification.encoding)) as f:
                        for scanCount, l in enumerate(f):
                            pass
            except:
                print("Exception scanning {0}".format(filename))
                print("{0}".format(l))
                raise
            
            
                
        elif specification.fullRegex is not None and specification.changeRegex is not None:
            
            #Full/change regexs available...
            scanCount=0
            if specification.encoding is None:
                scanFile=open(filename,"r")
            else:
                scanFile=open(filename,"r", encoding=specification.encoding)
            for line in scanFile:
                if identification is None:
                    if specification.compiledFullRegex.search(line) is not None:
                        identification='full'
                    elif specification.compiledChangeRegex.search(line) is not None:
                        identification='change'
                scanCount=scanCount+1        
            scanFile.close();  
              
            if identification is None:
                identification="error"
            
        if identification is None:
            identification = "undefined"
        return((scanCount, identification))
        
    
    
    def _turnArgsIntoFileList(self, suppliedFiles, recurse, filenameRegex):
        files = []    
        for thisFile in suppliedFiles:
    
            
            if filenameRegex is None:
                if thisFile.find("*") >-1:
                    (root,wildcard) =  os.path.split(thisFile)
                    
                    if recurse:
                        for root, dirnames, filenames in os.walk(root):
                            for filename in fnmatch.filter(filenames, wildcard):
                                fullFilename = os.path.join(root,filename)
                                files.append(fullFilename)
        
                    else:
                        filenames = os.listdir(root)
                        for filename in fnmatch.filter(filenames, wildcard):
                            fullFilename = os.path.join(root,filename)
                            files.append(fullFilename)
                        
                else:
                    files.append(thisFile)
            else:
    
                # Using a regex
        
                compiledRegex=re.compile(filenameRegex)    
    
                if recurse:
    
                    for root, dirnames, filenames in os.walk(thisFile):
                        for filename in filenames:
                            result = compiledRegex.search(filename)
                            if result is not None:
                                if result.group()==filename:
                                    fullFilename = os.path.join(root,filename)
                                    files.append(fullFilename)
        
                else:
                    filenames = os.listdir(thisFile)
                    for filename in filenames:
                        result = compiledRegex.search(filename)
                        if result is not None:
                            if result.group()==filename:
                                fullFilename = os.path.join(root,filename)
                                files.append(fullFilename)

        return(files)   
        