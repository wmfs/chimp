'''
Created on 16 Mar 2012

@author: Tim.Needham
'''
import cs
import json

def queueTasks(queuer, settings, stream, specificationRestriction, groupId, appLogger):
    appLogger.debug("")
    appLogger.debug("  Custom column tasks")
    appLogger.debug("  -------------------")
    

    
    sql = "select specification_name, source_schema,source_name,output_column_list,seq,(select max(seq) from calc.custom_registry as m where m.specification_name=r.specification_name and m.source_schema=r.source_schema and m.source_name=r.source_name) as max_seq from calc.custom_registry as r"
    if specificationRestriction is not None:
        sql += " where specification_name in({0})".format(specificationRestriction)
    sql += " order by specification_name,seq"

    queuer.supportCursor.execute(sql)
    specificationCustomSources = queuer.supportCursor.fetchall()
       
    for custom in specificationCustomSources:
        
        specificationName = custom[0]
        inputSourceSchema = custom[1]
        inputSourceName = custom[2]
        outputCustomList = custom[3].split(",")
        seq = custom[4]
        maxSeq = custom[5]
    
        processorFilename = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ("specifications", specificationName,"resources", "py","calculated"), "{0}_calculated_data_processor.py".format(inputSourceName))
        processorFilename = processorFilename.replace("\\", "\\\\") 
        
        
        args = {}
        args["inputSourceSchema"] = inputSourceSchema
        args["inputSourceName"] = inputSourceName
        args["customList"] = outputCustomList
        args["processorFilename"] = processorFilename
        args["flushQueue"] = (seq == maxSeq)      
        queuer.queue.queueTask(groupId, stream,  "syncCustomColumn", "Refresh custom columns {0} on {1}".format(outputCustomList,inputSourceName), None, None, None, json.dumps(args), False)
        appLogger.debug("      syncCustomColumn [{0}]".format(args))
        queuer.queue.queueCheckpoint(groupId, stream, "major", settings.args.tolerancelevel, queuer.commitFrequency, queuer.checkpointBehaviour)
        queuer.supportCursor.connection.commit()
        