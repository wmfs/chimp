import json
SOLR_SCHEMA="solr"
CALC_SCHEMA="calc"

def queueTasks(queuer, settings, stream, specificationRestriction, groupId, appLogger):
    appLogger.debug("")
    appLogger.debug("  Solr Document tasks")
    appLogger.debug("  -------------------")

    sql = "select specification_name,document_name,server_name,field_count from calc.solr_server_document_view"
    
    if specificationRestriction is not None:
      sql += " where specification_name in({0})".format(specificationRestriction)
      
    queuer.supportCursor.execute(sql)
    specificationSolrDocuments = queuer.supportCursor.fetchall()
    
    servers=[]
    
    for solrDocument in specificationSolrDocuments:
        
        specificationName = solrDocument[0]
        documentName = solrDocument[1]
        serverName = solrDocument[2]
        fieldCount = solrDocument[3]
        
        args = {}
        args["documentName"] = documentName
        args["serverName"] = serverName 
        args["fieldCount"] = fieldCount
        args["specification"] = specificationName
        
        queuer.queue.queueTask(groupId, stream,  "syncSolrDocuments", "Refresh '{0}' Solr document".format(documentName), None, None, None, json.dumps(args), False)
        appLogger.debug("      syncSolrDocuments [{0}]".format(args))
        queuer.queue.queueCheckpoint(groupId, stream, "major", settings.args.tolerancelevel, queuer.commitFrequency, queuer.checkpointBehaviour)
        queuer.supportCursor.connection.commit()
        
        if serverName not in servers:
            servers.append(serverName)
        
    for serverName in servers:
                
        args = {}
        args["serverName"] = serverName
        
        # Find if there's anything there already
        sql = "select exists(select 1 from {0}.{1} limit 1)".format(SOLR_SCHEMA, serverName)
        queuer.supportCursor.execute(sql)
        solrRecordsExist = queuer.supportCursor.fetchone()                
        if solrRecordsExist:
            command = "delta-import"
        else:
            command = "full-import"                
        args["command"] = command
        
        # Grab server URL
        sql = "select server_url from {0}.solr_server_registry where name=%s".format(CALC_SCHEMA)
        queuer.supportCursor.execute(sql,(serverName,))
        args["url"] = queuer.supportCursor.fetchone()[0]                
        
        
        queuer.queue.queueTask(groupId, stream,  "instructSolrServer", "Refresh of '{0}' Solr server".format(serverName), None, None, None, json.dumps(args), False)
        appLogger.debug("      instructSolrServer [{0}]".format(args))
        queuer.queue.queueCheckpoint(groupId, stream, "major", settings.args.tolerancelevel, queuer.commitFrequency, queuer.checkpointBehaviour)
        queuer.supportCursor.connection.commit()
        