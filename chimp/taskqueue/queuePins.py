'''
Created on 16 Mar 2012

@author: Tim.Needham
'''
import os
import cs
import json

def queueTasks(queuer, settings, stream, specificationRestriction, groupId, appLogger):
    appLogger.debug("")
    appLogger.debug("  Pin tasks")
    appLogger.debug("  ---------")

    sql = "select specification_name, pin_name, input_id_column, input_x_column, input_y_column, input_schema, input_source_name, input_column_list, output_column_list, where_clause from calc.pin_registry"
    if specificationRestriction is not None:
        sql += " where specification_name in ({0})".format(specificationRestriction)
    queuer.supportCursor.execute(sql)
    specificationPins = queuer.supportCursor.fetchall()
    
    for pin in specificationPins:
        specificationName = pin[0]
        pinName = pin[1]

        
        
        appLogger.debug("  * {0}".format(pinName))
        
        sql = "select pinhead.%s_exists()" %(pinName)
        queuer.supportCursor.execute(sql)        
        pinsExist = queuer.supportCursor.fetchone()[0]
        
        if not pinsExist:
            args = {}
            filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specification files",specificationName,"sql","indexes"), "drop_pinhead_%s_indexes.sql" % (pinName))
            appLogger.debug("      No pins... drop via '{0}'".format(filename))                                                                                    
            args["filename"] = filename
            queuer.queue.queueTask(groupId,  stream,  "script" , "Drop %s pin indexes" %(pinName), None, None, None, json.dumps(args), False)                                                        
            queuer.queue.queueCheckpoint(groupId, stream, "major", queuer.toleranceLevel, queuer.commitFrequency, queuer.checkpointBehaviour)                    

#        [0] specification_name
#        [1] pin_name
#        [2] input_id_column 
#        [3] input_x_column 
#        [4] input_y_column 
#        [5] input_schema
#        [6] input_source_name 
#        [7] input_column_list 
#        [8] output_column_list 
#        [9] where_clause
#        [10]processing_script_location 

        sourceName = pin[6]
    
        args = {}
        args["pinName"] = pinName
        args["inputIdColumn"] = pin[2]
        args["inputXColumn"] = pin[3]
        args["inputYColumn"] = pin[4]
        args["inputSchema"] = pin[5]
        
        args["inputSourceName"] = sourceName
        args["inputColumnList"] = pin[7]
        args["outputColumnList"] = pin[8]
        args["whereClause"] = pin[9]
        
        processorFilename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specification files", specificationName,"py","calculated"), "{0}_calculated_data_processor.py".format(sourceName))
        processorFilename = processorFilename.replace("\\", "\\\\") 
        args["processorFilename"] = processorFilename



        
        queuer.queue.queueTask(groupId, stream,  "syncPins", "Refresh %s pins" %(pinName), None, None, None, json.dumps(args), False)
        appLogger.debug("      syncPins [{0}]".format(args))

        if not pinsExist:
            args = {}
            filename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specification files",specificationName,"sql","indexes"), "create_pinhead_%s_indexes.sql" % (pinName))                                                                                    
            appLogger.debug("      Rebuild pins... via '{0}'".format(filename))
            args["filename"] = filename
            queuer.queue.queueTask(groupId,  stream, "script" , "Build %s pin indexes" %(pinName), None, None, None, json.dumps(args), False)            

        queuer.queue.queueCheckpoint(groupId, stream, "major", settings.args.tolerancelevel, queuer.commitFrequency, queuer.checkpointBehaviour)

        queuer.supportCursor.connection.commit()