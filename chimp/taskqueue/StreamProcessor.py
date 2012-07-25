'''
Created on 18 Jan 2012

@author: ryan
'''
import chimpspec
import json
import search
from calc.Pin import processSynchronizePins 
from calc.CustomColumn import processSynchronizeCustomColumn
from calc.Ctree import processCtrees
from calc.SolrDocument import processSolrDocuments
from calc.solr import processInstructSolrServer



import imp

class StreamProcessor:
    
    def processRecordTimestamp(self, queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, args):
        # Init
        lineCount = 0
        successCount = 1
        exceptionCount=0
        errorCount=0
        warningCount=0
        noticeCount=0
        ignoredCount=0   
        appLogger = settings.appLogger
    
        sql = "select now()"
        self.supportCursor.execute(sql)
        currentTimestamp = self.supportCursor.fetchone()[0]
        appLogger.debug("| currentTimestamp : {0}".format(currentTimestamp))    
        queue.finishTask(taskId, 1, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
        return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount, currentTimestamp) )


    def processVacuum(self, queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, args):
        # Init
        lineCount = 0
        successCount = 1
        exceptionCount=0
        errorCount=0
        warningCount=0
        noticeCount=0
        ignoredCount=0   
        appLogger = settings.appLogger
            
        schema = args["schema"]
        table = args["table"]
        
        sql = "vacuum analyze"
        if schema is not None and table is not None:
            sql += " {0}.{1}".format(schema, table) 
        queue.startTask(taskId, True)
        self.queue.setScanResults(taskId, 1)
        
        oldIsolationLevel = dataConnection.connection.isolation_level
        dataConnection.connection.set_isolation_level(0)
        dataCursor.execute(sql)        
        dataConnection.connection.set_isolation_level(oldIsolationLevel)
                             
        queue.finishTask(taskId, 1, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
        return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )

    
    def __init__(self, supportConnection, supportCursor, settings, queue, loader):
        self.settings = settings
        self.supportConnection = supportConnection
        self.supportCursor = supportCursor
        self.queue = queue
        self.loader = loader
        self.cachedSpecifications = {}
        self.cachedProcessors = {}                   
        
  
    def processStream(self, reset):   
    
        appLogger = self.settings.appLogger
        
        recordedTimestamp = None
        
        streamName = self.settings.args.streamname

        appLogger.info("")
        appLogger.info("Processing stream")
        appLogger.info("=================")
        appLogger.info("  streamName: {0}".format(streamName))
        appLogger.info("")
        
        loopConnection = self.settings.db.makeConnection("loop")
        
        dataConnection = self.settings.db.makeConnection("data")
        dataCursor = dataConnection.makeCursor("dataCursor", False, False)    
        
        keepTrying=True
        stopReason = None
        
        if reset:
            self.queue.rescheduleAll()
        
        sql = "select task_id, command,state,process_limit,args,label_short from shared.current_tasks where stream=%s and state != 'finished' order by task_id limit 1"
        
        successCountSinceCheckpoint = 0
        exceptionCountSinceCheckpoint = 0
        errorCountSinceCheckpoint = 0
        warningCountSinceCheckpoint = 0
        ignoredCountSinceCheckpoint = 0
        noticeCountSinceCheckpoint = 0

        while keepTrying:
            self.supportCursor.execute(sql,(streamName,))
            task = self.supportCursor.fetchone()        
            
            if task is not None:
                state = task[2]
                
                if state == "pending":
                    
                    dealtWith = False
                    successCount = None
                    exceptionCount = None
                    errorCount = None
                    warningCount = None
                    ignoredCount = None
                    noticeCount = None
                    
                    taskId = task[0]
                    command = task[1]                    
                    processLimit = task[3]
                    args = task[4]   
                    
                    if command != "checkpoint":
                        appLogger.info(" [TASK START]")
                        appLogger.info(" | {0}:".format(task[5]))
                        appLogger.info(" |   args: {0}".format(args))

                                 
                    if args is not None:
                        args= json.loads(args)
                    
                    if "specification" in args:
                        specification = self._getSpecification(args["specification"]);
                    if "processorFilename" in args:
                        processor = self._getProcessor(args["processorFilename"], args["inputSourceName"]);
                    
                    # Import commands                
                    if command=="script":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.loader.processScript(dataConnection, dataCursor, self.settings, taskId, processLimit, args)
                        dealtWith = True
                        
                    elif command=="stagecsv":                    
                        (identification, successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.loader.processStageCsv(dataConnection, dataCursor, taskId, processLimit, specification, args, appLogger)
                        if identification=="undefined":
                            dealtWith = True
                        elif identification=="full":
                            dealtWith = True
                        elif identification=="change":
                            dealtWith = True
                            
                    elif command=="stagejson":                        
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.loader.processStageJSON(dataConnection, dataCursor, taskId, processLimit, specification, args)
                        dealtWith = True
                                        
                    elif command=="callexternalloader":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.loader.processCallExternalLoader(taskId, processLimit, args)
                        dealtWith = True
                        
                    elif command=="sendtoimport":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.loader.processSendToImport(loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, specification, args)                                        
                        dealtWith = True
    
                    elif command=="importsyncdeletes":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.loader.processImportSyncDeletes(loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, specification, args)                                        
                        dealtWith = True    
                        
                    elif command=="sendtoeditable":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.loader.processSendToEditable(loopConnection,  dataConnection, dataCursor, self.settings, taskId, processLimit, specification, args)
                        dealtWith = True

                    elif command=="finisheditable":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.loader.processFinishEditable(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, args,recordedTimestamp)
                        dealtWith = True

                                    
                    elif command=="syncSearchSource":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = search.processSynchronizeSearchSource(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, specification, args)
                        dealtWith = True
                        
                    elif command=="syncPins":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = processSynchronizePins(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, args, processor)
                        dealtWith = True

                    elif command=="syncCustomColumn":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = processSynchronizeCustomColumn(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, args, processor)
                        dealtWith = True

                    elif command=="syncCtree":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = processCtrees(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, args)
                        dealtWith = True

                    elif command=="syncSolrDocuments":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = processSolrDocuments(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, args)
                        dealtWith = True

                    elif command=="instructSolrServer":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = processInstructSolrServer(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, args)
                        dealtWith = True

                    elif command=="vacuum":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) = self.processVacuum(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, args)
                        dealtWith = True

                    elif command=="recordtimestamp":
                        (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount, recordedTimestamp) = self.processRecordTimestamp(self.queue, self.supportConnection, self.supportCursor, loopConnection, dataConnection, dataCursor, self.settings, taskId, processLimit, args)
                        dealtWith = True
                                                
                    elif command=="checkpoint":
                        checkpointWantsToContinue = self.loader.processCheckpoint(dataConnection, dataCursor,streamName, self.settings, taskId, processLimit, args, successCountSinceCheckpoint, exceptionCountSinceCheckpoint, errorCountSinceCheckpoint, warningCountSinceCheckpoint, ignoredCountSinceCheckpoint, noticeCountSinceCheckpoint, appLogger)
                        if not checkpointWantsToContinue:
                            keepTrying = False
                        else:
                            successCountSinceCheckpoint = 0
                            exceptionCountSinceCheckpoint = 0
                            errorCountSinceCheckpoint = 0
                            warningCountSinceCheckpoint = 0
                            ignoredCountSinceCheckpoint = 0
                            noticeCountSinceCheckpoint = 0
                        dealtWith = True    
    
                    if command != "checkpoint":
                        if dealtWith:              
                            if successCount is not None:
                                successCountSinceCheckpoint = successCountSinceCheckpoint + successCount
                            if exceptionCount is not None:
                                exceptionCountSinceCheckpoint = exceptionCountSinceCheckpoint + exceptionCount
                            if errorCount is not None:
                                errorCountSinceCheckpoint = errorCountSinceCheckpoint + errorCount
                            if warningCount is not None:
                                warningCountSinceCheckpoint = warningCountSinceCheckpoint + warningCount
                            if ignoredCount is not None:
                                ignoredCountSinceCheckpoint = ignoredCountSinceCheckpoint + ignoredCount
                            if noticeCount is not None:
                                noticeCountSinceCheckpoint = noticeCountSinceCheckpoint + noticeCount
                                                                                                
                            self.queue.finishTask(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)                            
        
                        else:
                            # Not dealt with
                            keepTrying=False

                    if command=="checkpoint":
                        appLogger.info(" --<<CHECKPOINT: continue? {0}>>--".format(checkpointWantsToContinue))

                        if not checkpointWantsToContinue:
                            print("Checkpoint stopped queue prematurely:")
                            print("  toleranceLevel     : {0}".format(args["toleranceLevel"] ))
                            print("  checkpointBehaviour: {0}".format(args["checkpointBehaviour"] ))
                            print("  exceptionCount = {0}".format(exceptionCountSinceCheckpoint))
                            print("  errorCount     = {0}".format(errorCountSinceCheckpoint))
                            print("  warningCount   = {0}".format(warningCountSinceCheckpoint))
                            print("  noticeCount    = {0}".format(noticeCountSinceCheckpoint))
                            print("  ignoredCount   = {0}".format(ignoredCountSinceCheckpoint))
                            print("  successCount   = {0}".format(successCountSinceCheckpoint))
             
                    else:
                        if exceptionCount is not None and errorCount is not None and warningCount is not None:
                            if exceptionCount>0 or errorCount>0 or warningCount>0:
                                messagesSql = "select level,code,title,content from shared.task_messages where task_id=%s order by seq limit 10"
                                self.supportCursor.execute(messagesSql, (taskId,))
                                messages=self.supportCursor.fetchall()
                                for message in messages:
                                    appLogger.info(" |     {0} {1}: {2}".format(message[0], message[1], message[2]))
                                    appLogger.info(" |       > {0}".format(message[3]))
                                
                            appLogger.info(" [TASK END: success={0} exceptions={1}, errors={2}, warnings={3}, notices={4}, ignored={5}]".format(successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount))                            
                                                    
                        else:
                            appLogger.error("Unknown command '{0}'".format(command))
                            print ("Unknown command '{0}'".format(command))
                    appLogger.info("")
                else:
                    stopReason = "blocked"
                    keepTrying=False
    
            else:
                stopReason = "empty"
                keepTrying = False
        
        self.supportConnection.connection.commit()
        loopConnection.connection.close()
        dataConnection.connection.close()
        
        
    def _getSpecification(self, specificationName):
        if specificationName in self.cachedSpecifications:
            specification = self.cachedSpecifications[specificationName]
        else:
            specification = chimpspec.Spec(self.settings, specificationName, False)
            self.cachedSpecifications[specificationName] = specification            
        return specification        


    def _getProcessor(self, filename, sourceName):
        if filename in self.cachedProcessors:
            processor = self.cachedProcessors[filename]
        else:
            module = imp.load_source("{0}_calculated_data_processor.py".format(sourceName), filename)
            processor = module.DataCalculator()
            self.cachedProcessors[filename]=processor
        return(processor)
