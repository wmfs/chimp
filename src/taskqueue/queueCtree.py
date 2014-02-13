import json

def queueTasks(queuer, settings, schemaRestriction, stream, specificationRestriction, groupId, appLogger):
    appLogger.debug("")
    appLogger.debug("  Ctree tasks")
    appLogger.debug("  -----------")

    sql = "select specification_name,source_schema,source_name,ancestor_column_name,descendant_column_name,column_suffix,depth_column_name,immediate_ancestor_column_name,root_ancestor_column_name,descendant_count_column from calc.ctree_registry where "

    if specificationRestriction is not None:
        sql += "specification_name in({0})".format(specificationRestriction)

    if schemaRestriction is not None:
        if specificationRestriction is not None:
            sql += ' and '
        sql += " source_schema='{0}'".format(schemaRestriction)
        
    queuer.supportCursor.execute(sql)
    specificationCtrees = queuer.supportCursor.fetchall()
     
    appLogger.debug("  sql: {0}".format(sql));
    appLogger.debug("  schemaRestriction: {0}".format(schemaRestriction));
    
    for ctree in specificationCtrees:
        
    
        sourceSchema = ctree[1]
        sourceName = ctree[2]
        ancestorColumnName  =ctree[3]
        descendantColumnName=ctree[4]
        columnSuffix=ctree[5]
        depthColumnName=ctree[6]
        immediateAncestorColumnName=ctree[7]
        rootAncestorColumnName=ctree[8]
        descendantCountColumnName=ctree[9]
        
        args = {}
        args["inputSourceSchema"] = sourceSchema
        args["inputSourceName"] = sourceName 
        args["ancestorColumnName"] = ancestorColumnName
        args["descendantColumnName"] = descendantColumnName
        args["columnSuffix"] = columnSuffix
        args["depthColumnName"] = depthColumnName
        args["immediateAncestorColumnName"] = immediateAncestorColumnName
        args["rootAncestorColumnName"] = rootAncestorColumnName
        args["descendantCountColumnName"] = descendantCountColumnName
        
        queuer.queue.queueTask(groupId, stream,  "syncCtree", "Refresh {0} closure tree".format(sourceName), None, None, None, json.dumps(args), False)
        appLogger.debug("      syncCtree [{0}]".format(args))
        queuer.queue.queueCheckpoint(groupId, stream, "major", settings.args.tolerancelevel, queuer.commitFrequency, queuer.checkpointBehaviour)
        queuer.supportCursor.connection.commit()
        