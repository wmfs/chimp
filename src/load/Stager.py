'''
Created on 18 Jan 2012

@author: Ryan Pickett
'''
import cs
import imp
import datetime
import csv

class Stager:

    def __init__(self, queue, supportConnection, supportCursor, dataConnection, dataCursor, taskId, specification, paths, commitThreshold, appLogger):
        self.appLogger = appLogger
        self.commitThreshold = int(commitThreshold)
        self.queue = queue
        self.supportConnection = supportConnection
        self.supportCursor = supportCursor
        self.dataConnection = dataConnection
        self.dataCursor = dataCursor
        self.taskId = taskId
        self.specification = specification
        self.paths = paths
        
        # Prepare
        # =======
        self.lineCount = 0
        self.successCount=0
        self.exceptionCount = 0
        self.errorCount = 0
        self.warningCount = 0
        self.noticeCount = 0
        self.ignoredCount = 0
        self.action = None
        self.importData = []
        
        self.messageSql = "select shared.add_task_message(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
        self.transformFunctions={}
        for thisRecord in specification.records:
            if thisRecord.useful:
                
                moduleFilename = cs.getChimpScriptFilenameToUse(paths["repository"], ("specifications",specification.name,"resources", "py","transformation","stage"), "%s_stage_transformer.py" %(thisRecord.table))
                module = imp.load_source("%s_stage_transformer.py" %(thisRecord.table), moduleFilename)
                self.transformFunctions[thisRecord.table] = module.transformSuppliedValues
             
        
        #Set simple variables for speed 
        if specification.qualifier is not None:
            self.q = str(specification.qualifier)
        else:
            self.q = None            
        self.d = str(specification.delimiter)
        if len(specification.records) == 1:
            self.onlyOneRecord = True
        else:
            self.onlyOneRecord = False      
                

    def stageCSV(self, filename, lineLimit):
        
        # MAIN LOOP
        # ========
    
        if self.specification.encoding is None:
            f=open(filename,"r")
        else:
            f=open(filename, "r", encoding="%s"%(self.specification.encoding)) 
        
        for line in f:                                
    
            if self.specification.progressReportFrequency is not None:
                if self.lineCount % self.specification.progressReportFrequency == 0:                
                    self.queue.setTaskProgress(self.taskId, self.successCount, self.exceptionCount, self.errorCount, self.warningCount, self.noticeCount, self.ignoredCount)
            self.lineCount = self.lineCount + 1          
            if self.lineCount % self.commitThreshold == 0:
                self.appLogger.debug("| << Transaction size threshold reached ({0}): COMMIT >>".format(self.lineCount))
                self.dataConnection.connection.commit()

            
            matchFound = False
            formatted = line.strip("\n")          
            for thisRecord in self.specification.records:
                  
                #=========================================================
                #Should line be processed using this record specification?
                #=========================================================        
                
                processRecord = False 
                    
                if self.lineCount >= self.specification.startAtLine:             
                    if self.onlyOneRecord and thisRecord.compiledRecordRegex is None:
                        processRecord = True
                    else:
                        if thisRecord.compiledRecordRegex is not None:
                            if thisRecord.compiledRecordRegex.match(formatted) is not None:
                                processRecord = True
                                     
                # TODO: error handling on missing column mappings
                                      
                if processRecord:
    
                    matchFound = True
                    thisRecord.matches = thisRecord.matches + 1
                    
                    # Break line into columns
                    row = []
                    row.append(formatted)    
                    if self.q is not None:
                        csvReader = csv.reader(row, quotechar=self.q, delimiter=self.d)
                    else:
                        csvReader = csv.reader(row, delimiter=self.d)
                    
                    for row in csvReader:
                        
                        importData = []    
                        fieldIndex = 0
    
                        for thisField in thisRecord.fields:
                            if thisField.column  is not None:
                                
                                rawFieldValue = row[fieldIndex]
                                nativeFieldValue = None
                                                                          
                                if len(rawFieldValue) > 0:
                                    nativeFieldValue = self._getCSVNativeFieldValue(thisField.type, rawFieldValue, self.specification.encoding, thisField.decimalPlaces, self.specification.dateFormat, self.specification.timeFormat, self.specification.dateTimeFormat)                                        
    
                                importData.append(nativeFieldValue)
                                      
                            fieldIndex = fieldIndex + 1                            

                    # Decide on an action...                      
                    identification = self._getCSVRecordIdentification(thisRecord, formatted) 
    
                    try:
                        self._stageRecord(identification, thisRecord, importData)     
                    except Exception as detail:
                        print ("Original line:")
                        print (line)
                        print (thisRecord.stageCall)
                        print (importData)
                        raise                        
                   
            if not matchFound:
               
                self.queue.addTaskMessage(self.taskId, None, 0, "notice", "STAGE001", "Ignored record", None, 0, line)
                self.ignoredCount = self.ignoredCount + 1
        
            if lineLimit is not None:
                if self.lineCount == lineLimit:
                    break
        
        self.dataConnection.connection.commit()
    
    
    def stageJSON(self, recordIdentification, attributes):      
        importData = []
        record = self.specification.records[0]
        for field in record.fields:
            if field.column is not None:
                nativeFieldValue = None            
                if field.column in attributes:                    
                    rawFieldValue = attributes[field.column]                    
                    nativeFieldValue = self._getJSONNativeFieldValue(field.type, rawFieldValue, self.specification.encoding, field.decimalPlaces, self.specification.dateFormat, self.specification.timeFormat, self.specification.dateTimeFormat)
                    importData.append(nativeFieldValue)
                else:
                    importData.append(None)
                    
        try:
            self._stageRecord(recordIdentification, record, importData)     
        except Exception as detail:
            print ("JSON:")
            print (str(attributes))
            print (record.stageCall)
            print (importData)
            raise   
            

    def _stageRecord(self, identification, thisRecord, importData):
            
        # TRANSFORM   
        importData = self.transformFunctions[thisRecord.table](self.dataCursor, importData)
        
        # VALIDATE
        self.dataCursor.execute(thisRecord.validationCall, importData)
        messages = self.dataCursor.fetchall()
        messagesFound = False
        raisedWarning = False
        raisedError = False                
        raisedException=False
        
        for thisMessage in messages:
            messagesFound = True
            messageLevel = thisMessage[0]
            messageCode = thisMessage[1]
            messageTitle = thisMessage[2]
            messageAffectedColumns = thisMessage[3]
            messageAffectedRowCount = thisMessage[4]
            messageContent = "{0}\n\nOriginal line:\n{1}".format(thisMessage[5], importData)
            messageComponents = (self.taskId, thisRecord.table, self.lineCount, messageLevel,  messageCode, messageTitle,  messageAffectedColumns, messageAffectedRowCount, messageContent)
            self.dataCursor.execute(self.messageSql, (self.taskId, thisRecord.table, self.lineCount, messageLevel,  messageCode, messageTitle,  messageAffectedColumns, messageAffectedRowCount, messageContent))

            if messageLevel=="warning":
                raisedWarning = True
            elif messageLevel=="error":
                raisedError = True
            elif messageLevel=="exception":
                raisedException = True     
            elif messageLevel=="notice":
                self.noticeCount = self.noticeCount + 1
        
        # Add extra columns for stage statement
        importData.insert(0,self.taskId)
        importData.insert(0,self.lineCount)                
        importData.append(identification)

        if not (raisedError and raisedException):
            try:
    
                self.dataCursor.execute(thisRecord.stageCall, importData)
                messages = self.dataCursor.fetchall()
                for thisMessage in messages:
                    messagesFound = True
                    messageLevel = thisMessage[0]
                    messageCode = thisMessage[1]
                    messageTitle = thisMessage[2]
                    messageAffectedColumns = thisMessage[3]
                    messageAffectedRowCount = thisMessage[4]
                    messageContent = "{0}\n\nOriginal line:\n{1}".format(thisMessage[5], importData)
                    messageComponents = (self.taskId, thisRecord.table, self.lineCount, messageLevel,  messageCode, messageTitle,  messageAffectedColumns, messageAffectedRowCount, messageContent)
                    self.dataCursor.execute(self.messageSql, (self.taskId, thisRecord.table, self.lineCount, messageLevel,  messageCode, messageTitle,  messageAffectedColumns, messageAffectedRowCount, messageContent))
        
                    if messageLevel=="warning":
                        raisedWarning = True
                    elif messageLevel=="error":
                        raisedError = True
                    elif messageLevel=="exception":
                        raisedException = True     
                    elif messageLevel=="notice":
                        self.noticeCount = self.noticeCount + 1
                        
            except Exception as detail:
                print(thisRecord.stageCall)
                print(importData) 
                print(detail)
                raise

        if messagesFound:
            if raisedException:
                self.exceptionCount = self.exceptionCount +1
            elif raisedError:
                self.errorCount = self.errorCount +1
            elif raisedWarning:
                self.warningCount = self.warningCount +1
            else:
                self.successCount = self.successCount+1
        else:                                                                                    
            self.successCount = self.successCount+1
    

    def _getCSVRecordIdentification(self, thisRecord, formatted):
                
        if thisRecord.compiledInsertRegex is not None:                                                                        
            if thisRecord.compiledInsertRegex.match(formatted) is not None:
                return "insert"                                                                             

        if thisRecord.compiledMergeRegex is not None:                                                                        
            if thisRecord.compiledMergeRegex.match(formatted) is not None:
                return "merge"

        if thisRecord.compiledUpdateRegex is not None:                                                                        
            if thisRecord.compiledUpdateRegex.match(formatted) is not None:
                return "update"

        if thisRecord.compiledDeleteRegex is not None:                        
            if thisRecord.compiledDeleteRegex.match(formatted) is not None:
                return "delete"
                      
        return "undefined"
        
    
    def _getCSVNativeFieldValue(self, nativeDataType, rawValue, encoding, decimalPlaces, dateFormat, timeFormat, dateTimeFormat):
    
        if nativeDataType == "text":
            #http://www.stereoplex.com/2009/nov/8/python-unicode-and-unicodedecodeerror/
            #if encoding is not None:
            #    nativeFieldValue = rawValue.decode(encoding).encode("utf-8")
            #else:
            nativeFieldValue = rawValue
                
        if nativeDataType == "number":
            if decimalPlaces is None:
                if rawValue.isdigit():
                    nativeFieldValue = int(rawValue)
                else:
                    nativeFieldValue = None
            else:
                try:
                    nativeFieldValue = float(rawValue)
                except ValueError:
                    nativeFieldValue = None
                    
            #http://docs.python.org/library/time.html
        if nativeDataType == "date":
            dt = datetime.datetime.strptime(rawValue.strip(), dateFormat)
            nativeFieldValue = datetime.date(dt.year,dt.month,dt.day)
    
        if nativeDataType == "time":
            dt = datetime.datetime.strptime(rawValue.strip(), timeFormat)
            nativeFieldValue =  datetime.datetime.time(dt.hour, dt.minute, dt.second)
    
        if nativeDataType == "datetime":
            nativeFieldValue = datetime.datetime.strptime(rawValue.strip(), dateTimeFormat)
            
        return(nativeFieldValue)


    def _getJSONNativeFieldValue(self, nativeDataType, rawValue, encoding, decimalPlaces, dateFormat, timeFormat, dateTimeFormat):
        
        dateTimeFormat = "%Y-%m-%d %H:%M:%S"
    
        if nativeDataType == "text":
            return rawValue
    
        if nativeDataType == "number":
            if decimalPlaces is None:
                return int(rawValue)
            else:
                return float(rawValue)
            
        if nativeDataType == "date":
            return None if rawValue is None else datetime.datetime.strptime(rawValue, dateTimeFormat).date()
    
        if nativeDataType == "time":
            return None if rawValue is None else datetime.datetime.strptime(rawValue, dateTimeFormat).time()
    
        if nativeDataType == "datetime":
            return None if rawValue is None else datetime.datetime.strptime(rawValue, dateTimeFormat)
            
        return None

