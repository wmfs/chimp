'''
Created on 18 Jan 2012

@author: ryan
'''

import os
import platform
import json
import signal

class Queue:
    
    def __init__(self, connection, cursor, settings):
        self.conn = connection
        self.cursor = cursor

    def queueCheckpoint(self, groupId, stream, checkpointType, toleranceLevel, commitFrequency, checkpointBehaviour):
    
        if commitFrequency=="major":
            if checkpointType=="major":
                addToQueue=True
            else:
                addToQueue=False        
        elif commitFrequency=="minor":
            addToQueue=True
    
        if addToQueue:
            args = {}
            args["checkpointType"] = checkpointType 
            args["toleranceLevel"] = toleranceLevel
            args["checkpointBehaviour"] = checkpointBehaviour
            self.queueTask(groupId, stream, "checkpoint" , 'Checkpoint', None, None, None, json.dumps(args), False)

    def queueAVacuum(self, vacuumStrategy, groupId, stream, schema, table):
        if vacuumStrategy == "aggressive" or (vacuumStrategy == "progressive" and schema is not None and table is not None):
            args = {}
            args["schema"] = schema
            args["table"] = table
            if schema is not None and table is not None:
                label = "Vacuum table '{0}.{1}'".format(schema,table)
            else:
                label = "Vacuum database"
            self.queueTask(groupId, stream, "vacuum", label, None, None, 1, json.dumps(args), False)

                    
    def queueTask(self, groupId, stream, command, labelShort, labelLong, processLimit, scanCount, args, worthLogging):

        processId = None            
        auditComputerName = None
        auditOperatingSystem = None
        auditOperatingSystemRelease = None
        auditUsername = None        
        auditOperatingSystem=platform.system()
        auditOperatingSystemRelease = platform.release()    
        if auditOperatingSystem=="Windows":
            auditComputerName=os.getenv('COMPUTERNAME')
            auditUsername=os.getenv('USERNAME')
        elif auditOperatingSystem=="Linux":
            auditUsername = os.getenv('USER')
            auditComputerName = os.getenv('HOSTNAME')        
                       
        sql="select shared.queue_task(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        self.cursor.execute(sql,(groupId, stream, command, labelShort, labelLong, processId, processLimit, scanCount, auditComputerName, auditOperatingSystem, auditOperatingSystemRelease, auditUsername, args, worthLogging))
        taskId=int(self.cursor.fetchone()[0])
        self.cursor.connection.commit()
        return(taskId)            
    
    def startTask(self, taskId, scanRequired):
        processId = str(os.getpid())
        sql="select shared.set_task_start(%s,%s,%s)"
        self.cursor.execute(sql,(taskId, scanRequired, processId))
        self.cursor.connection.commit()
    
    def setScanResults(self, taskId, scanCount):
        sql="select shared.set_scan_results(%s,%s)"
        self.cursor.execute(sql,(taskId, scanCount))
        self.cursor.connection.commit()
    
    def setTaskProgress(self, taskId, latestSuccessCount, latestExceptionCount, latestErrorCount, latestWarningCount, latestNoticeCount, latestIgnoredCount):
        #shared.set_task_progress(p_task_id integer, p_latest_success_count integer, p_latest_exception_count integer, p_latest_error_count integer, p_latest_warning_count integer, p_latest_notice_count integer, p_latest_ignored_count integer)
        sql="select shared.set_task_progress(%s,%s,%s,%s,%s,%s,%s)"
        self.cursor.execute(sql,(taskId, latestSuccessCount, latestExceptionCount, latestErrorCount, latestWarningCount, latestNoticeCount, latestIgnoredCount))
        self.cursor.connection.commit()
    
    def finishTask(self, taskId, finalSuccessCount, finalExceptionCount, finalErrorCount, finalWarningCount, finalNoticeCount, finalIgnoredCount):
        sql="select shared.set_task_finish(%s,%s,%s,%s,%s,%s,%s)"
        self.cursor.execute(sql,(taskId, finalSuccessCount, finalExceptionCount, finalErrorCount, finalWarningCount, finalNoticeCount, finalIgnoredCount))
        self.cursor.connection.commit()
    
    def addTaskMessage(self, taskId, tableName, seq, level, code, title, affectedColumns, affectedRowCount, content):
        sql="select shared.add_task_message(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        self.cursor.execute(sql,(taskId, tableName, seq, level, code, title, affectedColumns, affectedRowCount, content))
        self.cursor.connection.commit()        

    def rescheduleAll(self):
        sql = "update shared.current_tasks set state='pending' where state != 'pending'"
        self.cursor.execute(sql)
        self.cursor.connection.commit()

    def clear(self):            
        self.cursor.execute("truncate table shared.current_tasks")
        self.cursor.connection.commit()

    def close(self):
        self.cursor.close()
        self.cursor.connection.close()

    def stop(self, stream):
        sql = "select process_id, task_id, command, label_short, label_long, started, scan_count, success_count + exception_count + error_count + warning_count AS completed_tasks_count from shared.current_tasks where stream=%s order by task_id limit 1"
        self.cursor.execute(sql,(stream,))
        currentTask = self.cursor.fetchone()
        
        if currentTask is not None:
            processId = currentTask[0]
            if processId is None:
                print("Unable to stop stream '{0}', task #{2} has no process_id".format(stream, currentTask[1]))
            else:
                processId = int(processId)
                os.kill(processId, signal.SIGINT)
                
                self.cursor.connection.commit()    
        