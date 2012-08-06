'''
Created on 17 Apr 2012

@author: Tim.Needham
'''
import re
import os
import cs
import xml.dom.minidom
from operator import attrgetter
from chimpsql import DBObjectRegistry
import datetime
#from chimpscript import registerAndWrite
import chimpscript
import chimpspec
import chimpsql
from calc.SolrAssemblers import SolrAssemblers
import http.client, urllib.parse
import json

CALC_SCHEMA = "calc"
SOLR_SCHEMA = "solr"
REFERENCE_SCHEMA = "reference"         

#
#
#        
#class SolrServerSQLBuilder:
#    def __init__(self, solrSettings, solrFields, solrServer):
#        self.solrSettings = solrSettings
#        self.solrFields = solrFields
#        self.solrServer = solrServer
#        
#        self.name = solrServer.name
#        self.solrServerUrl = solrServer.solrServerUrl
#        self.version = solrServer.version
#        self.connectionName = solrServer.connectionName
#        
#        self.tableName = solrServer.name
#        self.deletesTableName = "{0}_deletes".format(self.tableName)
#        self.chimpFields = self.solrFields.getChimpFields(self.name, self.solrServer.capabilities)
#        #self.usedColumns = self.solrFields.getUsedOrderedFieldList(self.capabilityNames, True)
#        
#    def getRegisterDML(self):
#        
#        return chimpsql.DML("SELECT {0}.register_solr_server('{1}', '{2}', '{3}', '{4}', '{5}');\n\n".format(CALC_SCHEMA, self.name, self.solrServerUrl, self.version, self.connectionName, ",".join(self.solrServer.capabilities)),
#                   dropDdl="SELECT {0}.unregister_solr_server('{1}');\n".format(CALC_SCHEMA, self.name))
#
#    def getDocumentSequence(self):
#        sequenceName = "{0}_seq".format(self.name)
#        return chimpsql.Sequence(sequenceName, SOLR_SCHEMA, 
#                        ("CREATE SEQUENCE {0}.{1}\n"
#                         "INCREMENT 1\n"
#                         "MINVALUE 1\n"
#                         "START 10\n"
#                         "CACHE 1;\n\n").format(SOLR_SCHEMA, sequenceName))
#
#
#    def getDocumentTable(self): 
#        ddl = "CREATE TABLE {0}.{1} (\n".format(SOLR_SCHEMA, self.tableName)
#        for field in self.chimpFields:
#            ddl += "{0},\n".format(field.columnClause("doc_id"))
#        ddl += "  modified timestamp with time zone NOT NULL default now());\n\n"
#        return chimpsql.Table(self.tableName, SOLR_SCHEMA, ddl)
#
#
#    def getModifiedIndex(self):
#        indexName = "{0}_modified_idx".format(self.name)
#        return chimpsql.Index(indexName, self.tableName, SOLR_SCHEMA,
#                     "CREATE INDEX {0} ON {1}.{2} (modified);\n\n".format(indexName, SOLR_SCHEMA, self.tableName))   
#
#    def getDeletesTable(self): 
#        for field in self.chimpFields:
#            if field.column == "doc_id":
#                chimpField = field                
#        # USED TO BE:chimpField.columnClause("doc_id")
#        ddl = ("CREATE TABLE {0}.{1} (\n"
#               "  {2});\n\n").format(SOLR_SCHEMA, self.deletesTableName, "USED TO BE?!")        
#        return chimpsql.Table(self.deletesTableName, SOLR_SCHEMA, ddl)
#
#
#    def getDocumentInsertFunction(self):
#        functionName = "{0}_insert".format(self.tableName)
#        parameters = []
#        for field in self.chimpFields:
#            if field.column != "doc_id":
#                parameters.append(chimpsql.SystemField("p_{0}".format(field.column), field.columnDataType))
#                
#        return chimpsql.Function(functionName, SOLR_SCHEMA,
#                [field.typeName for field in parameters],
#                ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
#                "  {2}\n"
#                ") RETURNS SETOF shared.chimp_message AS $$\n"
#                "DECLARE\n"
#                "  v_message record;\n"
#                "BEGIN\n"
#                "  INSERT INTO {0}.{3} (\n"
#                "    {4})\n"
#                "    VALUES (\n"
#                "    {5});\n" 
#                "EXCEPTION\n"
#                "  WHEN others THEN\n"
#                "    v_message = shared.make_exception('SOL001','Unhandled exception while inserting',NULL,1,SQLERRM);\n"
#                "    RETURN NEXT v_message;\n"
#                "END;\n"
#                "$$ LANGUAGE plpgsql;\n\n").format(SOLR_SCHEMA, 
#                                                   functionName,
#                                                   ",\n  ".join(["{0} {1}".format(field.column, field.typeName) for field in parameters]),
#                                                   self.tableName,        
#                                                   ",\n    ?????",
#                                                   ",\n    ?????"))



class SolrCapability:
    def __init__(self, capabilityTag):
        self.name = cs.grabAttribute(capabilityTag, "name")
        self.inputs = []
        self.delimiter = None
        self.format = None
        assemblerFunctionName="{0}Assembler".format(self.name)
        assemblers = SolrAssemblers()
        if not hasattr(assemblers, assemblerFunctionName):
            assemblerFunctionName = "defaultAssembler"        
        self.assemblerFunction = getattr(assemblers, assemblerFunctionName)

    def setCapabilityInputs(self, capabilityInputs):
        self.delimiter = capabilityInputs.delimiter
        self.format = capabilityInputs.format
        self.inputs = capabilityInputs.inputs
         
    def getColumns(self):
        r=[]
        for input in self.inputs:
            if input.column is not None:
                r.append(input.column)
        return(r)


    def debug(self, appLogger):
        appLogger.debug("   {0}".format(self.name))
        appLogger.debug("     delimiter = {0}".format(self.delimiter))
        appLogger.debug("     format    = {0}".format(self.format))
        for input in self.inputs:
            appLogger.debug("     [column={0} optionSetName={1} optionSetColumn={2} constant={3}]".format(input.column, input.optionSetName, input.optionSetColumn, input.constant))


class SolrFields:

    class SolrField:
            
                    
        def debug(self, appLogger):
            appLogger.debug("  {0}:".format(self.name))
            appLogger.debug("    [solrType={0} indexed={1} stored={2} mandatory={3} chimpType={4} size={5} decimalPlaces={6} capability={7}]".format(self.solrType, self.indexed, self.stored, self.mandatory, self.chimpType, self.size, self.decimalPlaces, self.capability))
            appLogger.debug("")
            
        def __init__(self, fieldTag, order):
            self.name = cs.grabAttribute(fieldTag, "name")
            self.order = order
            self.solrType = cs.grabAttribute(fieldTag, "solrType")
            self.indexed = cs.grabAttribute(fieldTag, "indexed")
            if self.indexed in("true","True"):
                self.indexed = True
            else:
                self.indexed = False
            self.stored = cs.grabAttribute(fieldTag, "stored")
            if self.stored in("true","True"):
                self.stored = True
            else:
                self.stored = False
            self.capability = cs.grabAttribute(fieldTag, "capability")

            self.chimpType = cs.grabAttribute(fieldTag, "chimpType")
            self.size = cs.grabAttribute(fieldTag, "size")
            if self.size is not None:
                self.size = int(self.size)

            self.decimalPlaces = cs.grabAttribute(fieldTag, "decimalPlaces")
            self.decimalPlaces = cs.grabAttribute(fieldTag, "decimalPlaces")
            if self.decimalPlaces is not None:
                self.decimalPlaces = int(self.decimalPlaces)

            
            self.mandatory = cs.grabAttribute(fieldTag, "mandatory")
            if self.mandatory is None:
                self.mandatory = False
            else:
                if self.mandatory in("True","true","TRUE"):
                    self.mandatory = True
                elif self.mandatory in("False","false","FALSE"):
                    self.mandatory = False
            self.variable = cs.grabAttribute(fieldTag, "variable")

    def getCapabilityOrder(self, capability):
        i = 0
        order = None
        for field in self.getOrderedFieldList():
            if field.capability == capability and order is None:
                order = i
            i=i+1                        
        return(order)
    
#    def getOrderedFieldList(self):
#        r = sorted(self.fields.values(), key=attrgetter('order'))
#        return(r)
#
#    def getUsedOrderedFieldList(self, capabilityNames, includeDocId):
#        used = []
#        ordered = self.getOrderedFieldList()
#        for field in ordered:
#            if field.capability is None or (field.capability is not None and field.capability in capabilityNames):
#                if field.name=="doc_id":
#                    if includeDocId:
#                        used.append(field.name)
#                else:
#                    used.append(field.name)
#        return(used)


        

#    def getUsedDelimitedColumnNames(self, capabilities):
#        used = self.getUsedOrderedFieldList(capabilities, True)
#        r = ",".join(used)
#        return(r)



    def debug(self, appLogger):
        appLogger.debug("")
        appLogger.debug("Solr Fields")
        appLogger.debug("-----------")
        appLogger.debug("")

        for field in self.fields:
            field.debug(appLogger)

    def __init__(self, settings):        
        filename = os.path.join(settings.paths["config"], "solr", "solr-fields.xml")
        xmldoc = xml.dom.minidom.parse(filename)        
        solrFieldsTag = xmldoc.getElementsByTagName("solrFields")[0]
        fieldConfigTag = solrFieldsTag.getElementsByTagName("fieldConfig")[0]
        fieldTags = fieldConfigTag.getElementsByTagName("field")
        self.fields = []
        i=0
        for field in fieldTags:
            f = self.SolrField(field, i)
            i=i+1
            self.fields.append(f)
        xmldoc.unlink()
                        
    

class SolrSettings:
    
    def debug(self, appLogger):
        appLogger.debug("")
        appLogger.debug("Solr Settings")
        appLogger.debug("-------------")
        appLogger.debug("  registry : {0}".format(self.registry))
        appLogger.debug("")
        
    def __init__(self, settings):
        
        filename = os.path.join(settings.paths["config"], "solr", "solr-settings.xml")                
        xmldoc = xml.dom.minidom.parse(filename)        
        solrSettingsTag = xmldoc.getElementsByTagName("solrSettings")[0]
            
        registryTag = solrSettingsTag.getElementsByTagName("registry")[0]
        
        self.registry = {}
        keys = registryTag.getElementsByTagName("key")
        for key in keys:
            name = cs.grabAttribute(key, "name")
            value = cs.grabAttribute(key, "value")            
            self.registry[name]=value        
        
        xmldoc.unlink()
    
    
class SolrServer:

    class ServerField:
        def __init__(self, solrServerName, solrField):
            
            self.name = solrField.name
            self.solrType = solrField.solrType
            self.indexed = solrField.indexed
            self.stored = solrField.stored
            self.capability = solrField.capability
            self.chimpSpecificationType = solrField.chimpType            
            self.size = solrField.size
            self.decimalPlaces = solrField.decimalPlaces
            self.mandatory = solrField.mandatory
            self.variable = solrField.variable

            if self.name=="document_id":
                defaultClause = "nextval('{0}.{1}_seq')::character varying".format(SOLR_SCHEMA, solrServerName)
            else:
                defaultClause = None
            self.chimpField= chimpspec.SpecificationRecordField(None, None, column=self.name, type=self.chimpSpecificationType, size=self.size, decimalPlaces=self.decimalPlaces, mandatory=self.mandatory, default=defaultClause)

            self.columnDataType = self.chimpField.columnDataType

#            moduleFilename = cs.getChimpScriptFilenameToUse(repositoryPath, ["specification files", dataSpecification, "py", "search formatting"], "%s_search_formatter.py" %(tableName))
#            module = imp.load_source("%s_search_formatter.py" %(tableName), moduleFilename)
#            defaultFunctions = module.DefaultSearchProcessor()
#    
#            if hasattr(defaultFunctions, 'getUrn'):
#                getUrn = defaultFunctions.getUrn

            
        def debug(self, appLogger):
            appLogger.debug("   {0}".format(self.name))
            
    def __init__(self, settings, serverName, solrFields):
        
        self.name = serverName
        
        # Parse XML        
        filename = os.path.join(settings.paths["repository"], "solr_servers", serverName, "solr_server.xml")                
        xmldoc = xml.dom.minidom.parse(filename)
        solrServerTag = xmldoc.getElementsByTagName("solrServer")[0]
        solrTag = solrServerTag.getElementsByTagName("solr")[0]
        self.version = cs.grabAttribute(solrTag, "version")
        self.solrServerUrl = cs.grabAttribute(solrTag, "url")
        self.profile = cs.grabAttribute(solrTag, "profile")
        self.connectionName = cs.grabAttribute(solrTag, "connection")


        # Grab capabilities
        self.capabilities = []
        capabilitiesTag = solrServerTag.getElementsByTagName("capabilities")[0]
        capabilityTags = capabilitiesTag.getElementsByTagName("capability")        
        for capabilityTag in capabilityTags:                    
            self.capabilities.append(SolrCapability(capabilityTag))

        
        # Build a field list
        self.fields = []
        for solrField in solrFields.fields:
            
            if solrField.capability is None or (solrField.capability is not None and solrField.capability in(self.getCapabilityNames())):
                self.fields.append(self.ServerField(self.name, solrField))
        
    
        xmldoc.unlink()

        filename = os.path.join(settings.paths["config"], "connections", "{0}.xml".format(self.connectionName))                
        xmldoc = xml.dom.minidom.parse(filename)

        connectionTag = xmldoc.getElementsByTagName("connection")[0]
        self.dbHost = cs.grabAttribute(connectionTag, "host")
        self.dbName = cs.grabAttribute(connectionTag, "dbname")
        self.dbUser = cs.grabAttribute(connectionTag, "user")
        self.dbPassword = cs.grabAttribute(connectionTag, "password")
        self.dbPort = cs.grabAttribute(connectionTag, "port")
                
        xmldoc.unlink()


    def setCapabilityInputs(self, capabilityInputs):
        for capability in self.capabilities:
            if capability.name==capabilityInputs.capabilityName:
                capability.setCapabilityInputs(capabilityInputs)


    def getAllColumns(self):
        columns=["visibility","security"]
        for capability in self.capabilities:
            for input in capability.inputs:
                if input.column is not None:
                    columns.append(input.column)                    
        columns = list(set(columns))
        return(columns)

    def getSelectList(self, sourceFields):
        allDocumentColumns = self.getAllColumns()
        # Order them as per specification...
        selectList = ["id"]        
        for field in sourceFields:
            if field.column is not None:
                if field.column in allDocumentColumns:
                    selectList.append(field.column)
        selectList.append("visibility")
        selectList.append("security")

        return(selectList)

    def getSchemaFieldTags(self, sourceFields):
        
        def getTagString(name, type, stored, indexed, required):
            r = '     <field name="{0}" type="{1}" stored="{2}" indexed="{3}" required="{4}" />\n'.format(name,type,str(stored).lower(),str(indexed).lower(),str(required).lower())
            return(r)
                
        script = ""
        capabilityList = self.getCapabilityNames()
        for field in sourceFields.fields:
            if field.capability is None or (field.capability is not None and field.capability in capabilityList):
                script+=  getTagString(field.name, field.solrType, field.stored, field.indexed, field.mandatory)

        return(script)
        
        
    def getReferenceTableInfo(self):
        referenceTables=[]
        for capability in self.capabilities:
            for input in capability.inputs:
                if input.optionSetName is not None:
                    referenceTables.append((input.optionSetName, input.column, input.optionSetColumn))
        referenceTables = list(set(referenceTables))
        return(referenceTables)       

    def getCapabilityByName(self, capabilityName):
        for capability in self.capabilities:
            if capability.name == capabilityName:
                r = capability
        return(r)
    
    def getCapabilityNames(self):
        l=[]
        for capability in self.capabilities:        
            l.append(capability.name)
        return (l)

    def getFieldsByCapability(self, capabilityName):
        r=[]
        for field in self.fields:
            if field.capability == capabilityName:
                r.append(field)
        return(r)        

        
    def getRegisterDML(self):        
        fieldCount = len(self.fields)
        return chimpsql.DML("SELECT {0}.register_solr_server('{1}', '{2}', '{3}', '{4}', '{5}',{6});\n\n".format(CALC_SCHEMA, self.name, self.solrServerUrl, self.version, self.connectionName, ",".join(self.getCapabilityNames()), fieldCount),
                   dropDdl="SELECT {0}.unregister_solr_server('{1}');\n".format(CALC_SCHEMA, self.name))

    def getDocumentSequence(self):
        sequenceName = "{0}_seq".format(self.name)
        return chimpsql.Sequence(sequenceName, SOLR_SCHEMA, 
                        ("CREATE SEQUENCE {0}.{1}\n"
                         "INCREMENT 1\n"
                         "MINVALUE 1\n"
                         "START 10\n"
                         "CACHE 1;\n\n").format(SOLR_SCHEMA, sequenceName))


    def getDocumentTable(self): 
        ddl = "CREATE TABLE {0}.{1} (\n".format(SOLR_SCHEMA, self.name)
        for field in self.fields:
            ddl += "{0},\n".format(field.chimpField.columnClause("document_id"))
        ddl += "  modified timestamp with time zone NOT NULL default now());\n\n"
        return chimpsql.Table(self.name, SOLR_SCHEMA, ddl)


    def getModifiedIndex(self):
        indexName = "{0}_modified_idx".format(self.name)
        return chimpsql.Index(indexName, self.name, SOLR_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} (modified);\n\n".format(indexName, SOLR_SCHEMA, self.name))   
    def getDocumentTypeIndex(self):
        indexName = "{0}_document_type_idx".format(self.name)
        return chimpsql.Index(indexName, self.name, SOLR_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} (document_type);\n\n".format(indexName, SOLR_SCHEMA, self.name))   


    def getKeyIndex(self):
        indexName = "{0}_document_key_idx".format(self.name)
        return chimpsql.Index(indexName, self.name, SOLR_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} (document_type, document_key);\n\n".format(indexName, SOLR_SCHEMA, self.name))   

    def getDeletesTable(self):
        tableName =  "{0}_deletes".format(self.name)
 
        for field in self.fields:
            if field.name == "document_id":
                chimpField = field.chimpField                
        ddl = ("CREATE TABLE {0}.{1} (\n"
               "{2},\n"
               "  deleted_at timestamp with time zone NOT NULL DEFAULT now());\n\n").format(SOLR_SCHEMA, tableName, chimpField.columnClause("document_id"))        
        return chimpsql.Table(tableName, SOLR_SCHEMA, ddl)

    def getDeletedAtIndex(self):
        tableName =  "{0}_deletes".format(self.name)
        indexName = "{0}_deleted_at_idx".format(self.name)
        return chimpsql.Index(indexName, tableName, SOLR_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} (deleted_at);\n\n".format(indexName, SOLR_SCHEMA, tableName))   


    def getQueueDeleteFunction(self):
        functionName = "queue_delete_of_{0}".format(self.name)         
        return chimpsql.Function(functionName, SOLR_SCHEMA, 
                        [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"            
                         "BEGIN\n"
                         "  INSERT INTO {0}.{2}_deletes (document_id)\n"
                         "  VALUES (old.document_id);\n"
                         "  RETURN old;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(SOLR_SCHEMA, 
                                                            functionName,
                                                            self.name))

    def getQueueDeleteTrigger(self, documentTable, triggerFunction):
        triggerName = "queue_deletion_of_{0}_insert".format(self.name)
        return chimpsql.Trigger(triggerName, documentTable.name, triggerFunction.name, triggerFunction.schema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER DELETE ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, triggerFunction.schema, documentTable.name, triggerFunction.schema, triggerFunction.name))


    def getDocumentApplyFunction(self):
        functionName = "apply_{0}".format(self.name)
        
        parameters = []
        targetList = []
        for field in self.fields:
            if field.name != "document_id":
                targetList.append(field.name)
                parameters.append(chimpsql.SystemField("p_{0}".format(field.name), field.columnDataType))
        
        return chimpsql.Function(functionName, SOLR_SCHEMA,
                [field.typeName for field in parameters],
                ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
                "  {2},\n"
                "  p_attempt_delete BOOLEAN) RETURNS SETOF shared.chimp_message AS $$\n"
                "DECLARE\n"
                "  v_message record;\n"
                "BEGIN\n"
                "  IF p_attempt_delete THEN\n"
                "    DELETE FROM {0}.{3}\n"
                "    WHERE document_key = p_document_key\n"
                "    AND document_type = p_document_type;\n"
                "  END IF;\n"
                "  INSERT INTO {0}.{3} (\n"
                "    {4})\n"
                "    VALUES (\n"
                "    {5});\n" 
                "EXCEPTION\n"
                "  WHEN others THEN\n"
                "    v_message = shared.make_exception('SOL001','Unhandled exception while inserting',NULL,1,SQLERRM);\n"
                "    RETURN NEXT v_message;\n"
                "END;\n"
                "$$ LANGUAGE plpgsql;\n\n").format(SOLR_SCHEMA, 
                                                   functionName,
                                                   ",\n  ".join(["{0} {1}".format(field.column, field.typeName) for field in parameters]),
                                                   self.name,        
                                                   ",\n    ".join(targetList),
                                                   ",\n    ".join(map(lambda col: "p_{0}".format(col), targetList))))
    
    #map(lambda field: ",\n    {0}".format(field.column), EDITABLE_SYSTEM_FIELDS)),
    
    def generateInstallScript(self, repositoryPath, solrSettings, solrFields):
        installFilename = os.path.join(repositoryPath, "solr_servers", self.name, "install", "install_{0}.sql".format(self.name))        
        file=open(installFilename, "w")    
        objectRegistry = DBObjectRegistry()
            
        file.write("-- Script to install objects to support\n")
        file.write("-- the Solr server found here: {0}\n".format(self.solrServerUrl))
        now = datetime.datetime.now()
        file.write("-- Generated by Chimp "+now.strftime("%d-%m-%Y %H:%M:%S")+"\n")
        file.write("--\n")
        file.write("-- Fields:\n")
        for field in self.fields:
            file.write("--  {0}\n".format(field.name))
        file.write("\n\n\n")
        
        chimpscript.registerAndWrite(self.getRegisterDML(), objectRegistry, file)
        chimpscript.registerAndWrite(self.getDocumentSequence(), objectRegistry, file) 
        
        documentTable = self.getDocumentTable()             
        chimpscript.registerAndWrite(documentTable, objectRegistry, file)
        chimpscript.registerAndWrite(self.getModifiedIndex(), objectRegistry, file)
        chimpscript.registerAndWrite(self.getDocumentTypeIndex(), objectRegistry, file)
        chimpscript.registerAndWrite(self.getKeyIndex(), objectRegistry, file)
        
        deletesTable = self.getDeletesTable()
        chimpscript.registerAndWrite(deletesTable, objectRegistry, file)
        chimpscript.registerAndWrite(self.getDeletedAtIndex(), objectRegistry, file)
        
        deleteQueueFunction = self.getQueueDeleteFunction()
        chimpscript.registerAndWrite(deleteQueueFunction, objectRegistry, file)
        
        chimpscript.registerAndWrite(self.getQueueDeleteTrigger(documentTable, deleteQueueFunction), objectRegistry, file)
        
        chimpscript.registerAndWrite(self.getDocumentApplyFunction(), objectRegistry, file)
                
        file.close()

        dropFilename = os.path.join(repositoryPath, "solr_servers", self.name, "install", "drop_{0}.sql".format(self.name))        
        objectRegistry.writeDropScript(dropFilename)

        # Build index scripts
        # -------------------
        tableList = []
        for index in objectRegistry.indexes:
            if index.tableName not in tableList:
                tableList.append(index.schema + "." + index.tableName)

        for thisTable in tableList:                    
            filename = "drop_{0}_indexes.sql".format(str(thisTable).replace(".", "_"))
            filename = os.path.join(repositoryPath, "scripts",  "generated", "solr server files", self.name, "indexes", filename)
            dropFile = open(filename, "w")

            filename = "create_{0}_indexes.sql".format(str(thisTable).replace(".", "_"))
            filename = os.path.join(repositoryPath, "scripts",  "generated", "solr server files", self.name, "indexes", filename)
            createFile = open(filename,"w")

            for index in objectRegistry.indexes:             
                if thisTable == "{0}.{1}".format(index.schema, index.tableName):
                    dropFile.write("{0}\n".format(index.getDropStatement()))
                    createFile.write("{0}{1}\n".format("" if index.droppable else "-- ", index.ddl))

            dropFile.close();    
            createFile.close();
        
        return((dropFilename, installFilename))            


#    def clearExportDirectory(self, appLogger):
#        exportDirectory = os.path.join(self.configPath, "solr", "export")
#        appLogger.debug("exportDirectory: {0}".format(exportDirectory))        
#        files = [ f for f in os.listdir(exportDirectory) if not f.endswith(".svn")]
#        for f in files:
#            fullFile = os.path.join(exportDirectory, f)
#            appLogger.debug("  Deleting {0}".format(fullFile))
#            os.remove(fullFile)


#
#        filename= os.path.join(settings.paths["repository"],"scripts", "generated",  "search domain files", self.name, "sql", "indexes", "drop_search_%s_indexes.sql" %(self.name))             
#        dropFile=open(filename,"w")
#
#
#        filename= os.path.join(settings.paths["repository"],"scripts", "generated",  "search domain files", self.name, "sql", "indexes", "create_search_%s_indexes.sql" %(self.name))             
#        createFile=open(filename,"w")
#                   
#        for thisIndex in self.indexList:
#            dropFile.write("DROP INDEX IF EXISTS %s.%s;\n" %(thisIndex[3], thisIndex[1]))                        
#            createFile.write("%s\n" %(thisIndex[2]))
#
#    
#        dropFile.close();    
#        createFile.close();
#        
#        return BuildScriptResult(filename=outputFilename, errorsFound=False, warningsFound=False)
#    

    def generateSchema(self, solrSettings, solrFields, configPath, repositoryPath, appLogger):
        
        #Get version
        if self.version is None:
            version = self.version
        else:
            version = solrSettings.registry["defaultVersion"]

        # Establish source file
        sourceFilename = os.path.join(configPath, "solr", "versions", version, "schema.xml")                
        sourceFile = open(sourceFilename, "r")
        
        # Establish target file
        targetFilename = os.path.join(repositoryPath, "solr_servers", self.name, "export", "schema.xml")                            
        targetFile = open(targetFilename, "w")
        
        replicate = True
        
        fieldsStart=re.compile("^\s*<\s*fields\s*>")
        fieldsEnd=re.compile("^\s*<\s*/\s*fields\s*>")
        uniqueKey=re.compile("^\s*<\s*uniqueKey\s*>")
        defaultSearchField=re.compile("^\s*<\s*defaultSearchField\s*>")
                    
        for line in sourceFile:

            if fieldsStart.search(line) is not None:
                replicate=False
                
                # Switch replication off and inject new stuff
                fieldsTag=(" <fields>\n"
                           "   <!-- Generated by Chimp -->\n"
                           "{0}"
                           " </fields>\n".format(self.getSchemaFieldTags(solrFields)))
                targetFile.write(fieldsTag)
                
            if replicate:
                
                if uniqueKey.search(line) is not None:
                    targetFile.write(" <uniqueKey>document_id</uniqueKey>\n")
                    
                elif defaultSearchField.search(line) is not None:
                    targetFile.write(" <defaultSearchField>search_text</defaultSearchField>\n")                    
                else:
                    targetFile.write(line)

            if fieldsEnd.search(line) is not None:
                replicate=True
                                 
        # Tidy up
        sourceFile.close()        
        targetFile.close()

    def generateSolrConfig(self, solrSettings, configPath, repositoryPath, appLogger):

        #Get version
        if self.version is None:
            version = self.version
        else:
            version = solrSettings.registry["defaultVersion"]

        sourceFilename = os.path.join(configPath, "solr", "versions", version, "solrconfig.xml")                
        sourceFile = open(sourceFilename, "r")
        
        
        filename = os.path.join(repositoryPath, "solr_servers", self.name, "export", "solr_config.xml")
        outputFile = open(filename, "w")
        
        for line in sourceFile:
            outputFile.write(line)
        
        outputFile.close()
        sourceFile.close()

    def generateDataConfig(self, solrSettings, solrFields, repositoryPath, appLogger):
        filename = os.path.join(repositoryPath, "solr_servers", self.name, "export", "data-config.xml")
        file = open(filename, "w")
        
        url="{0}//{1}:{2}/{3}".format(solrSettings.registry["databaseUrlPrefix"], self.dbHost, self.dbPort, self.dbName) 
        
        fullImportSql = "select {0} from {1}.{2}".format(", ".join(map(lambda field:field.name, self.fields)), SOLR_SCHEMA, self.name)
        deltaImportQuerySql = "%s where document_id='${dataimporter.delta.document_id}'" %(fullImportSql)
        deltaQuery ="select document_id from %s.%s where modified &gt; '${dataimporter.last_index_time}'" %(SOLR_SCHEMA, self.name)
        deletedPkQuery = "select document_id from %s.%s_deletes where deleted_at &gt; '${dataimporter.last_index_time}'"%(SOLR_SCHEMA, self.name)
        
        file.write(('<dataConfig>\n'
                    '    <dataSource type="{0}" driver="{1}"\n'
                    '        url="{2}" user="{3}" password="{4}" />\n'
                    '    <document>\n'
                    '        <entity\n'
                    '          name="chimp_{5}"\n'
                    '          query="{6}"\n'
                    '          deltaImportQuery="{7}"\n'
                    '          deltaQuery="{8}"\n'
                    '          deletedPkQuery="{9}" />\n'
                    '    </document>\n'
                    '</dataConfig>\n'.format(solrSettings.registry["dataSource"], #0
                                             solrSettings.registry["databaseDriver"], #1
                                             url, #2
                                             self.dbUser, #3
                                             self.dbPassword, #4
                                             self.name, #5
                                             fullImportSql, #6
                                             deltaImportQuerySql, #7
                                             deltaQuery, #8
                                             deletedPkQuery))) #9

        file.close()

    
    def debug(self, appLogger):
        appLogger.debug("")  
        appLogger.debug("SolrServer") 
        appLogger.debug("----------")
        appLogger.debug("Server:")
        appLogger.debug("  version           : {0}".format(self.version))
        appLogger.debug("  solrServerUrl     : {0}".format(self.solrServerUrl))
        appLogger.debug("  profile           : {0}".format(self.profile))
        appLogger.debug("  connectionName    : {0}".format(self.connectionName))
        appLogger.debug("")
        appLogger.debug("  Database:")
        appLogger.debug("    dbHost     : {0}".format(self.dbHost))
        appLogger.debug("    dbName     : {0}".format(self.dbName))
        appLogger.debug("    dbUser     : {0}".format(self.dbUser))
        appLogger.debug("    dbPort     : {0}".format(self.dbPort))
        if self.dbPassword is not None:
            appLogger.debug("    dbPassword : [supplied]")
        else:
            appLogger.debug("    dbPassword : [NOT supplied]")            
        appLogger.debug("")
        appLogger.debug("Capabilities:")
        for capability in self.capabilities:
            capability.debug(appLogger)

        appLogger.debug("")
        appLogger.debug("Fields:")        
        for field in self.fields:
            field.debug(appLogger)
        
        appLogger.debug("")
        
def processInstructSolrServer(queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, args):
    
    # Init
    lineCount = 0
    successCount = 0
    exceptionCount=0
    errorCount=0
    warningCount=0
    noticeCount=0
    ignoredCount=0   
    appLogger = settings.appLogger

    queue.startTask(taskId, True)
        
    serverName = args["serverName"]
    command=args["command"]

    serverUrl = args["url"]
    urlComponents = urllib.parse.urlparse(serverUrl)   

    params = urllib.parse.urlencode({"command":command})    
    conn = http.client.HTTPConnection(urlComponents.netloc)
    requestUrl = "{0}/dataimport?{1}".format(urlComponents.path, params)

    appLogger.debug("|   requestUrl: {0}".format(requestUrl))

    try:
        conn.request("POST", requestUrl)
        response = conn.getresponse()    
        appLogger.debug("|   status    : {0}".format(response.status))
        data = response.read()
        data = data.decode("utf-8")
        appLogger.debug("|   response  : {0}".format(data))
    except Exception as detail:
        appLogger.debug("|   REQUEST FAILED: {0}".format(detail))
        warningCount = 1
        queue.addTaskMessage(taskId, None, 0, "warning", "SOLR001", "Could not instruct Solr server", None, 0, "{0} ({1})".format(detail, requestUrl))
        
    queue.finishTask(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
    return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )
        