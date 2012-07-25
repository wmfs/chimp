import imp
import os
import cs
import xml.dom.minidom
from operator import attrgetter
from chimpsql import DBObjectRegistry
import datetime
import chimpspec
import chimpsql
import calc.solr as solr

CALC_SCHEMA = "calc"
SOLR_SCHEMA = "solr"  
REFERENCE_SCHEMA = "reference" 

class CapabilityInputs:
    
    class Input:
        def __init__(self, inputTag):
            self.column = cs.grabAttribute(inputTag, "column")
            self.optionSetName = cs.grabAttribute(inputTag, "optionSetName")
            self.optionSetColumn = cs.grabAttribute(inputTag, "optionSetColumn")
            self.constant = cs.grabAttribute(inputTag, "constant")                

            self.formattingFunctions = []
            self.formattingMethods = []
            formattingTag = inputTag.getElementsByTagName("formatting")
            if len(formattingTag) >0:
                formattingTag = formattingTag[0]
                
                functionTags = formattingTag.getElementsByTagName("function")
                for functionTag in functionTags:
                    self.formattingFunctions.append(cs.grabAttribute(functionTag, "name"))        

                methodTags = formattingTag.getElementsByTagName("method")
                for methodTag in methodTags:
                    self.formattingMethods.append(cs.grabAttribute(methodTag, "name"))        

                        
        def wrapReturnValueInFormatting(self, intendedReturnValue):
            wrapped=""
            for function in self.formattingFunctions:
                wrapped += "{0}(".format(function)
            wrapped += intendedReturnValue
            
            for method in self.formattingMethods:
                wrapped += ".{0}() if {1} is not None else None".format(method, intendedReturnValue)           
            i=0
            while i<len(self.formattingFunctions):
                wrapped += ")"
                i+=1
            return(wrapped)               
    
    def __init__(self, capabilityInputsTag):
        self.capabilityName = cs.grabAttribute(capabilityInputsTag, "capability")
        self.delimiter = cs.grabAttribute(capabilityInputsTag, "delimiter")
        self.format = cs.grabAttribute(capabilityInputsTag, "format")
        self.inputs = []
        inputTags = capabilityInputsTag.getElementsByTagName("input")
        for inputTag in inputTags:
            self.inputs.append(self.Input(inputTag))
        


           
class SolrDocument:

#    def filteredFields(self, solrFields, capabilityName):
#        r = {}
#        for fieldName in solrFields.getUsedOrderedFieldList([capabilityName], False):
#            field = solrFields.fields[fieldName]
#            if field.capability == capabilityName:
#                r[field.name]=field
#        return(r)
            
    def __init__(self, solrDocumentTag, solrSettings, solrFields, settings):
        
        
        self.type = "solrDocument"        
        self.taskOrder = 100
        self.name = cs.grabAttribute(solrDocumentTag, "documentName")                
        self.solrServerName = cs.grabAttribute(solrDocumentTag, "solrServerName")
        self.solrServer = solr.SolrServer(settings, self.solrServerName, solrFields)
        
        self.triggeringColumns = []

        documentContentTag = solrDocumentTag.getElementsByTagName("documentContent")
        if documentContentTag is not None:
            documentContentTag = documentContentTag [0]
            capabilityInputsTags = documentContentTag.getElementsByTagName("capabilityInputs")

            for capabilityInputsTag in capabilityInputsTags:
                capabilityInputs = CapabilityInputs(capabilityInputsTag)
                self.solrServer.setCapabilityInputs(capabilityInputs)


            keyCapability = self.solrServer.getCapabilityByName("documentKey")            
            self.keyColumns = keyCapability.getColumns()
            #self.keyFields = keyCapability.getFields()
#            for column in self.keyColumns:
#                field = self.solrFields.
#            self.keyFields = self.solrServer.getFieldsByCapability("documentKey")
#        self.constructors = {}
#
#        for capability in self.server.capabilities:
#            if capability=="docKey":
#                constructor = construct.Construct_docKey(solrFields.getCapabilityOrder(capability), findInput("doc_key"), self.filteredFields(solrFields,capability))
#            elif capability=="docRanking":
#                constructor = construct.Construct_documentRanking(solrFields.getCapabilityOrder(capability), findInput("docRanking"), self.filteredFields(solrFields,capability))
#            elif capability=="zone":
#                constructor = construct.Construct_zone(solrFields.getCapabilityOrder(capability), findInput("zone"), self.filteredFields(solrFields,capability))
#            elif capability=="urn":
#                constructor = construct.Construct_urn(solrFields.getCapabilityOrder(capability), findInput("urn"), self.filteredFields(solrFields,capability))
#            elif capability=="icon":
#                constructor = construct.Construct_icon(solrFields.getCapabilityOrder(capability), findInput("icon"), self.filteredFields(solrFields,capability))
#            elif capability=="coordinates":
#                constructor = construct.Construct_coordinates(solrFields.getCapabilityOrder(capability), findInput("coordinates"), self.filteredFields(solrFields,capability))
#            elif capability=="synopsis":
#                constructor = construct.Construct_synopsis(solrFields.getCapabilityOrder(capability), findInput("synopsis"), self.filteredFields(solrFields,capability))
#            elif capability=="classification":
#                constructor = construct.Construct_classification(solrFields.getCapabilityOrder(capability), findInput("classification"), self.filteredFields(solrFields,capability))
#            elif capability=="ctree":
#                constructor = construct.Construct_ctree(solrFields.getCapabilityOrder(capability), findInput("ctree"), self.filteredFields(solrFields,capability))
#            elif capability=="language":
#                constructor = construct.Construct_language(solrFields.getCapabilityOrder(capability), findInput("language"), self.filteredFields(solrFields,capability))
#            elif capability=="eventDate":
#                constructor = construct.Construct_eventDate(solrFields.getCapabilityOrder(capability), findInput("eventDate"), self.filteredFields(solrFields,capability))
#            elif capability=="containerDoc":
#                constructor = construct.Construct_containerDocument(solrFields.getCapabilityOrder(capability), findInput("containerDocument"), self.filteredFields(solrFields,capability))
#            elif capability=="visibility":
#                constructor = construct.Construct_visibility(solrFields.getCapabilityOrder(capability), findInput("visibility"), self.filteredFields(solrFields,capability))
#            elif capability=="security":
#                constructor = construct.Construct_security(solrFields.getCapabilityOrder(capability), findInput("security"), self.filteredFields(solrFields,capability))
#            self.constructors[capability]=constructor
#    
            
    def debug(self, appLogger):
        appLogger.debug("    solrDocument")
        appLogger.debug("      name              : {0}".format(self.name))
        appLogger.debug("      solrServerName    : {0}".format(self.solrServerName))
        appLogger.debug("      keyColumns        : [{0}]".format(", ".join(self.keyColumns)))        
        self.solrServer.debug(appLogger)
        #for constructor in self.constructors.values():
        #    constructor.debug(appLogger)

        
    def getTriggeringColumns(self):        
        return(self.solrServer.getAllColumns())                           
        
    def getExtraSystemFields(self):
        extraSystemFields = []
        return(extraSystemFields)    

    def requiresFile(self):
        return(False)

    def getSolrDocumentRegistrationDML(self, schemaName, specificationName, sourceName, scriptPath):
        return chimpsql.DML(("SELECT {0}.register_solr_document('{1}', '{2}', '{3}', '{4}', '{5}');\n\n").format(CALC_SCHEMA, 
                                                 self.name, 
                                                 self.solrServerName, 
                                                 specificationName,
                                                 schemaName,
                                                 sourceName),
                    dropDdl="SELECT {0}.unregister_solr_document('{1}');\n".format(CALC_SCHEMA,  self.name))

    def getSolrDocumentQueueTable(self, fields):
        tableName = "{0}_solr_document_queue".format(self.name)
        self.chimpKeyFields = []
        columnClauseList = []

        for field in fields:
            if field.column in self.keyColumns:
                self.chimpKeyFields.append(field)
                columnClauseList.append(field.columnClause(None))
        
        
        ddl = ( "CREATE TABLE {0}.{1} (\n{2},\n"
                "  PRIMARY KEY({3}));\n\n".format(CALC_SCHEMA, tableName, ",\n".join(columnClauseList), ",".join(self.keyColumns)))        
        return chimpsql.Table(tableName, CALC_SCHEMA, ddl)

    def getSolrDocumentQueueFunction(self, ddlOperation, records):
        functionName = "add_to_{0}_solr_document_queue_{1}".format(self.name, ddlOperation)
        

        ddl = ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                             "  RETURNS trigger AS\n"
                             "$BODY$\n"
                             "DECLARE\n"
                             "  v_exists BOOLEAN;\n"
                             "BEGIN\n").format(CALC_SCHEMA, functionName)
        for record in records:
            whereClause = " AND ".join(map(lambda field: "{0}={1}.{0}".format(field.column, record), self.chimpKeyFields))
            targetColumns = ", ".join(map(lambda field: "{0}".format(field.column), self.chimpKeyFields))
            sourceClause = ", ".join(map(lambda field: "{0}.{1}".format(record, field.column), self.chimpKeyFields))            
            ddl += ("  SELECT exists(SELECT 1 FROM {0}.{2}_solr_document_queue WHERE {3})\n"
                    "  INTO v_exists;\n"
                    "  IF NOT v_exists THEN\n"
                    "    INSERT INTO {0}.{2}_solr_document_queue ({4})\n"
                    "    VALUES ({5});\n"
                    "  END IF;\n").format(CALC_SCHEMA, functionName, self.name, whereClause, targetColumns, sourceClause)

        ddl += ("  RETURN new;\n"                                    
                "END;\n"
                "$BODY$\n"
                "LANGUAGE plpgsql;\n\n")
        
        return chimpsql.Function(functionName, CALC_SCHEMA, [],ddl)



    def getSolrDocumentQueueInsertTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "q_add_{0}_insert_to_solr_document_queue".format(self.name)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER INSERT\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, sourceSchema, sourceName, CALC_SCHEMA, triggerFunction.name))

    def getSolrDocumentQueueDeleteTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "q_add_{0}_delete_to_solr_document_queue".format(self.name)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER DELETE\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, sourceSchema, sourceName, CALC_SCHEMA, triggerFunction.name))


    def getSolrDocumentQueueUpdateTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "q_add_{0}_update_to_solr_document_queue".format(self.name)
        triggeringColumns = self.getTriggeringColumns()
        ofClause = ", ".join(triggeringColumns)
        whenClause=" OR ".join(map(lambda field: "old.{0} IS DISTINCT FROM new.{0}".format(field), triggeringColumns))        
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER UPDATE\n"
                        "OF {1}\n"
                        "ON {2}.{3}\n"
                        "FOR EACH ROW\n"
                        "WHEN ({4})\n"
                        "EXECUTE PROCEDURE {5}.{6}();\n\n").format(triggerName, ofClause, sourceSchema, sourceName, whenClause, CALC_SCHEMA, triggerFunction.name))

    def getSolrQueueView(self, sourceSchema, sourceName, fields):
            viewName = "{0}_solr_document_queue_view".format(self.name)
            selectClauses = self.solrServer.getSelectList(fields)

            
            #print(selectClauses)
            # Additional reference select clauses
        
            
            
            # Calculate if there are any additional reference tables to join
            referenceTableList = self.solrServer.getReferenceTableInfo()            
            referenceTableSql = ""
            refCount=1
            for referenceTable in referenceTableList:
                referenceTableSql += "\n    LEFT JOIN {0}.{1} AS ref{2} ON source.{3} = ref{2}.value".format(REFERENCE_SCHEMA, referenceTable[0], refCount,referenceTable[1])
                refCount +=1
            
            referenceSelectSql = ""
            refCount=1
            for referenceTable in referenceTableList:
                referenceSelectSql += ",\n    ref{0}.{1} AS {2}_{3}".format(refCount, referenceTable[2], referenceTable[0],referenceTable[2])
                refCount +=1
            
            
            return chimpsql.View(viewName, CALC_SCHEMA, 
                        ("CREATE OR REPLACE VIEW {0}.{1} AS\n"
                         "  SELECT\n"
                         "    source.{2}{8}\n"
                         "  FROM {3}.{4} AS source\n"
                         "    JOIN {0}.{5}_solr_document_queue USING ({6})"
                         "{7};\n\n").format(CALC_SCHEMA, 
                                         viewName,
                                         ",\n    source.".join(selectClauses),
                                         sourceSchema,
                                         sourceName,
                                         self.name,
                                         ", ".join(self.keyColumns),
                                         referenceTableSql,
                                         referenceSelectSql))



    def buildSolrDocumentScript(self, paths, specificationName, solrFields, defaultVisibility, defaultSecurity, srid):
        
        scriptFilename= os.path.join(paths["repository"], "scripts", "generated", "specification files", specificationName, "py", "solr formatting", "{0}_document_formatter.py".format(self.name))        
        scriptFile=open(scriptFilename,"w")
        
        #standardArgs="dbCursor, raw, entry, optionSets"
        
        script = ("import chimpsolrformattingtools as tools\n\n"
                  "# Generated Solr document formatter methods\n"
                  "# -----------------------------------------\n\n"
                  "# Methods prepared to take raw data from a database record and\n"
                  "# produce formatted data for use by the '{0}' Solr server.\n\n"
                  "class DocumentFormatter:\n\n"
                  "    def getSolrDocument(self, dbCursor, sourceRow):\n").format(self.solrServer.solrServerUrl)

        for capability in self.solrServer.capabilities:
            script+= capability.assemblerFunction(self.name, capability,self.solrServer.fields, defaultVisibility, defaultSecurity, srid)
        
        script += ("\n\n        # Initialize values to send back\n"                   
                   "        # ------------------------------\n")
        for field in self.solrServer.fields:
            if field.name != "document_id":
                script += "        self.{0} = None\n".format(field.variable)

        script += ("\n\n        # Call the various methods\n"
                   "        # ----------------------\n")
        for capability in self.solrServer.capabilities:
            script+= "        {0}Formatter()\n".format(capability.name)


        script += ("\n\n        # And return back values\n"
                   "        # ----------------------\n"
                   "        return((self.{0}))".format(",\n                self.".join(filter(lambda column:column!="documentId", map(lambda field:field.variable, self.solrServer.fields)))))
                   
 
#        solrRecordColumns = solrFields.getUsedOrderedFieldList(self.server.capabilities, False)
#        i=0
#        for column in solrRecordColumns:
#            script += "#  [{0}] {1}\n".format(i, column) 
#            i += 1
#                         
#        script += "\n\nclass SolrDocumentFormatter:\n\n"        
#        
#        for constructor in self.constructors.values():
#            print("{0} {1}".format(constructor.order, constructor.name))
#        
#        for constructor in sorted(self.constructors.values(), key=lambda x: x.order):
#            script = script + constructor.getProcessorScript(solrRecordColumns).replace("\t","    ")
#        
#        
        scriptFile.write(script)
        
        scriptFile.close()

def processSolrDocuments(queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, args):
    # Init
    lineCount = 0
    successCount = 0
    exceptionCount=0
    errorCount=0
    warningCount=0
    noticeCount=0
    ignoredCount=0   
    appLogger = settings.appLogger
    commitThreshold = int(settings.env["dataCommitThreshold"])

    messageSql = "select shared.add_task_message(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        
    documentName = args["documentName"]
    serverName = args["serverName"]
    fieldCount = args["fieldCount"] - 1
    filename = "{0}_document_formatter.py".format(documentName)
    moduleToUse = cs.getChimpScriptFilenameToUse(settings.paths["repository"], ["specification files", args["specification"], "py", "solr formatting"], filename)
    module = imp.load_source(filename, moduleToUse)
    conversionFunctions = module.DocumentFormatter()
    conversionFunction = conversionFunctions.getSolrDocument
    
    # Publish count
    queue.startTask(taskId, True)
    sql = "select count(*) from {0}.{1}_solr_document_queue_view".format(CALC_SCHEMA, documentName)
    supportCursor.execute(sql)
    documentCount = supportCursor.fetchone()[0]
    queue.setScanResults(taskId, documentCount)
    appLogger.info(" |   documentCount : {0}".format(documentCount))

    sql = "select exists(select 1 from {0}.{1} where document_type=%s limit 1)".format(SOLR_SCHEMA, serverName)
    supportCursor.execute(sql,(documentName,))
    documentsExist = supportCursor.fetchone()[0]
    appLogger.info(" |   documentsExist: {0}".format(documentsExist))


    # Apply 
    applySql = "select * from {0}.apply_{1}(".format(SOLR_SCHEMA, serverName)
    i=0
    while i<fieldCount:
        applySql += "%s,"
        i += 1
    if documentsExist:
        applySql += "true)"
    else:
        applySql += "false)"

    # Establish main loop
    loopSql = "select * from {1}.{2}_solr_document_queue_view as a".format(None, CALC_SCHEMA, documentName)
    appLogger.info(" |   loopSql    : {0}".format(loopSql))   
    loopCursor = loopConnection.makeCursor("solr", True, True)
    loopCursor.execute(loopSql)
    lineCount=0
    
    # Truncate table
    truncateDml = "delete from {0}.{1}_solr_document_queue".format(CALC_SCHEMA, documentName)
    appLogger.info(" |   truncateDml : {0}".format(truncateDml))
    for record in loopCursor:
        if lineCount%1000 ==0:
            queue.setTaskProgress(taskId, successCount, 0, 0, 0, 0, 0)
        lineCount=lineCount+1
        if lineCount % commitThreshold == 0:
            appLogger.debug("| << Transaction size threshold reached ({0}): COMMIT >>".format(lineCount))
            dataConnection.connection.commit()
        solrDocument = conversionFunction(supportCursor, record)        
        dataCursor.execute(applySql, solrDocument)
        

        
        messages = dataCursor.fetchall()
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
            messageContent = "{0}\n\nDocument data being applied:\n{1}".format(thisMessage[5], solrDocument)
            supportCursor.execute(messageSql, (taskId, None, lineCount, messageLevel,  messageCode, messageTitle,  messageAffectedColumns, messageAffectedRowCount, messageContent))

            if messageLevel=="warning":
                raisedWarning = True
            elif messageLevel=="error":
                raisedError = True
            elif messageLevel=="exception":
                raisedException = True     
            elif messageLevel=="notice":
                noticeCount = noticeCount + 1

        if messagesFound:
            if raisedException:
                exceptionCount = exceptionCount +1
            elif raisedError:
                errorCount = errorCount +1
            elif raisedWarning:
                warningCount = warningCount +1
            else:
                successCount = successCount+1
        else:                                                                                    
            successCount = successCount+1
    
    loopCursor.close()
    
    dataCursor.execute(truncateDml)
    
    queue.finishTask(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
    return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )


