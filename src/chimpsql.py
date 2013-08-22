import itertools

MV_SCHEMA = "mv"
AREA_SCHEMA = "areas"
PINHEAD_SCHEMA = "pinhead"
SHARED_SCHEMA = "shared"
STAGE_SCHEMA = "stage"
VERSION_CONTROL_SCHEMA = "vc"
WORKING_SCHEMA = "working"
HISTORY_SCHEMA = "history"
IMPORT_SCHEMA = "import"
EDITABLE_SCHEMA = "editable"
ALERTS_SCHEMA = "alerts"



#    def getSolrTable(self, ):
#        schema = self.specification.dedicatedStagingAreaName if self.specification.dedicatedStagingAreaName is not None else STAGE_SCHEMA
#        tableName = record.getNativeTable()
#        return Table(tableName, schema,
#                    ("CREATE TABLE {0}.{1}(\n"
#                    "  id bigint PRIMARY KEY DEFAULT nextval('shared.{2}_seq'){3},\n"
#                    "  identification character varying(10) NOT NULL DEFAULT 'undefined',\n"
#                    "  task_id integer not null DEFAULT 0\n"
#                    "){4};\n\n").format(schema, tableName, record.table, 
#                                        self._getColumnDefsSQL(filter(lambda field: field.dumpDataType != "geometry", record.getInputMappedFields())), 
#                                        self._getWithOIDSSQL(record)))



class SpecificationSQLBuilder:
    
    def __init__(self, specification):
        self.specification = specification

    #VC        
    def getVersionControlCheckedOutTable(self, record):
        tableName = "checked_out_{0}".format(record.table)         
        return Table(tableName, VERSION_CONTROL_SCHEMA, 
                     ("CREATE TABLE {0}.{1}(\n"
                      "  id bigint PRIMARY KEY, \n"
                      "  editable_record_id bigint NOT NULL,\n"
                      "  checkout_author character varying (200) NOT NULL{2},\n"
                      "  checkout_created timestamp with time zone NOT NULL DEFAULT now(),\n"
                      "  checkout_modified timestamp with time zone NOT NULL DEFAULT now()\n"
                      "){3};\n\n").format(VERSION_CONTROL_SCHEMA, tableName, self._getColumnDefsSQL(record.getAllMappedFields()), self._getTableParameters(False, record.withOids, None, None)))
        
    def getVersionControlCheckedOutAuthorIndex(self, record, table):
        indexName = "vc_{0}_checked_out_author".format(record.table)        
        return Index(indexName, table.name, table.schema, 
                     "CREATE INDEX {0} ON {1}.{2} (editable_record_id, checkout_author);\n\n".format(indexName, VERSION_CONTROL_SCHEMA, table.name))

    # SHARED
    def getSharedSpecificationRegisterDML(self):
        return DML(("SELECT {0}.register_specification('{1}', '{2}', {3}, {4}, {5}"
                    ");\n\n").format(SHARED_SCHEMA, self.specification.name, self.specification.label,
                                    "NULL" if self.specification.vendor is None else "'{0}'".format(self.specification.vendor),
                                    "NULL" if self.specification.version is None else "'{0}'".format(self.specification.version),
                                    "NULL" if self.specification.fileWildcard is None else "'{0}'".format(self.specification.fileWildcard)),
                                                                        
                   dropDdl=("SELECT {0}.unregister_specification('{1}');\n"
                            "SELECT {0}.unregister_dataitems('{1}');\n").format(SHARED_SCHEMA, self.specification.name))

    def getDataitemRegisterDML(self, record, field):
        
        if field.label is not None:
            label = field.label.replace("'","''")
            label = "'{0}'".format(label)
        else:
            label = "NULL"

        if field.label is not None:
            label = field.label.replace("'","''")
            label = "'{0}'".format(label)
        else:
            label = "NULL"

        if field.description is not None:
            description = field.description.replace("'","''")
            description = "'{0}'".format(description)
        else:
            description = "NULL"

        if field.size is None:
            size = 'NULL'
        else:
            size = field.size

        if field.decimalPlaces is None:
            decimalPlaces = 'NULL'
        else:
            decimalPlaces = field.decimalPlaces
            
        if len(field.tags)>0:
            tags="'{0}'".format(",".join(map(lambda x:"''{0}''".format(x),field.tags)))
        else:
            tags="NULL"

        if record.editable:
            editable = "true"
        else:
            editable = "false"
        
        return DML("SELECT {0}.register_dataitem('{1}', '{2}', '{3}', '{4}', {5}, {6}, '{7}', {8}, {9}, {10}, {11});\n".format(
                            SHARED_SCHEMA, #0
                            self.specification.name, #1 
                            field.dataitemName, #2
                            record.table, #3
                            field.column, #4
                            editable, #5
                            label, #6
                            field.type, #7 data_type
                            size, #8 size
                            decimalPlaces,#9 decimal_places
                            description, #10
                            tags)) #11


    def getSharedSequence(self, record):
        sequenceName = "{0}_seq".format(record.table)
        return Sequence(sequenceName, SHARED_SCHEMA, 
                        ("CREATE SEQUENCE {0}.{1}\n"
                         "INCREMENT 1\n"
                         "MINVALUE 1\n"
                         "START 10\n"
                         "CACHE 1;\n\n").format(SHARED_SCHEMA, sequenceName))
        
    def getSharedNextIdFunction(self, record, sequence):
        functionName = "get_next_{0}_id".format(record.table)
        return Function(functionName, SHARED_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS bigint AS $$\n"
                         "  DECLARE\n"
                         "    v_id bigint;\n"
                         "  BEGIN\n"
                         "    SELECT nextval('{2}.{3}')\n"
                         "    INTO v_id;\n"
                         "    RETURN v_id;\n"
                         "  END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(SHARED_SCHEMA, functionName, sequence.schema, sequence.name))   

    # WORKING
    def getWorkingView(self, record):
        return View(record.table, WORKING_SCHEMA, 
                    ("CREATE OR REPLACE VIEW {0}.{1} AS\n"
                     "SELECT *\n"
                     "FROM {2}.{1};\n\n".format(WORKING_SCHEMA, record.table, record.getWorkingTargetSchema())))

    def getWorkingRemoteInsertRule(self, record, view):
        ruleName = "{0}_working_remote_insert".format(record.table)
        nextIdFunctionName = "get_next_{0}_id".format(record.table)
        
        editableFieldNames = ""
        editableFieldValues = ""
        
        if record.getWorkingTargetSchema() == "editable":
            editableFieldNames = (",\n"
                                  "  original_source,\n"
                                  "  latest_source")
            editableFieldValues = (",\n"
                                   "  'working',\n"
                                   "  'working'")                 

        return Rule(ruleName, 
                    ("CREATE OR REPLACE RULE {0} AS\n" 
                   "ON INSERT TO {1}.{2} DO INSTEAD \n"
                   "INSERT INTO {3}.{4} (\n"
                   "  id{5}{6},\n"
                   "  visibility,\n"                                        
                   "  security)\n"
                   "VALUES (\n"
                   "  {7}.{8}(){9}{10},\n" 
                   "  new.visibility,\n"
                   "  new.security\n"
                   ");\n\n").format(ruleName, view.schema, view.name,
                                    record.getWorkingTargetSchema(), record.table,
                                    "".join(map(lambda field: ",\n  {0}".format(field.column), record.getAllMappedFields())), 
                                    editableFieldNames,
                                    SHARED_SCHEMA, nextIdFunctionName,
                                    "".join(map(lambda field: ",\n  new.{0}".format(field.column), record.getAllMappedFields())), 
                                    editableFieldValues))

    def getWorkingRemoteUpdateRule(self, record, view):
        ruleName = "{0}_working_remote_update".format(record.table)
        return Rule(ruleName, 
                    ("CREATE OR REPLACE RULE {0} AS\n" 
                     "ON UPDATE TO {1}.{2} DO INSTEAD\n"
                     "UPDATE {3}.{4} SET\n"
                     "  visibility=new.visibility,\n"                                        
                     "  security=new.security{5}\n"
                     "WHERE {6}.id=old.id;\n\n").format(ruleName, view.schema, view.name,
                                                        record.getWorkingTargetSchema(), record.table,
                                                        "".join(map(lambda field: ",\n  {0}=new.{0}".format(field.column), record.getAllMappedFields())),
                                                        record.table))
    
    def getWorkingRemoteDeleteRule(self, record, view):
        ruleName = "{0}_working_remote_delete".format(record.table)
        return Rule(ruleName,
                    ("CREATE OR REPLACE RULE {0} AS\n"
                     "ON DELETE TO {1}.{2} DO INSTEAD \n"
                     "UPDATE {3}.{4} SET visibility=10\n"
                     "WHERE {5}.id=old.id;\n\n".format(ruleName, view.schema, view.name,
                                                       record.getWorkingTargetSchema(), record.table,
                                                       record.table)))


    # STAGE
    def getStageNativeSchema(self):
        schemaName = self.specification.dedicatedStagingAreaName
        return Schema(schemaName, "CREATE SCHEMA {0};\n\n".format(schemaName))
    
    def getStageSequencerSequence(self):
        sequenceName = "{0}_sequencer_seq".format(self.specification.name)         #
        return Sequence(sequenceName, STAGE_SCHEMA, 
                        "CREATE SEQUENCE {0}.{1} START WITH 1;\n\n".format(STAGE_SCHEMA, sequenceName))

    def getStageSequencerTable(self, sequence):
        tableName = "{0}_sequencer".format(self.specification.name)        
        return Table(tableName, STAGE_SCHEMA,
                     ("CREATE TABLE {0}.{1}(\n"
                      "  table_name character varying(200) NOT NULL,\n"
                      "  seq integer NOT NULL DEFAULT nextval('{2}.{3}'),\n"
                      " record_id bigint NOT NULL\n"
                      ");\n\n").format(STAGE_SCHEMA, tableName, sequence.schema, sequence.name))
    
    def getStageSequencerTruncateDML(self, table, sequence):
        return DML(("TRUNCATE {0}.{1};\n"
                    "ALTER SEQUENCE {2}.{3} RESTART WITH 1;\n").format(table.schema, table.name, sequence.schema, sequence.name))
    
    def getStageSequencerIndex(self, table):
        indexName = "{0}_sequencer_idx".format(self.specification.name)                
        return Index(indexName, table.name, STAGE_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2}(seq);\n\n".format(indexName, STAGE_SCHEMA, table.name))

    def getStageSequencerAddFunction(self, table):
        functionName = "add_to_{0}_sequencer".format(self.specification.name)
        return Function(functionName, STAGE_SCHEMA, ["character varying", "bigint"],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}(p_table_name character varying, p_record_id bigint)\n"
                         "  RETURNS void AS\n"
                         "$BODY$\n"
                         "BEGIN\n"
                         "  INSERT INTO {2}.{3} (\n"
                         "    table_name,\n"
                         "    record_id)\n"
                         "  VALUES (\n"
                         "    p_table_name,\n"
                         "    p_record_id);\n"                        
                         "END;\n"
                         "$BODY$\n"
                         "  LANGUAGE plpgsql;\n\n").format(STAGE_SCHEMA, functionName, table.schema, table.name))

    def getStageSequencerAddTriggerFunction(self, record, delegate):
        functionName = "add_{0}_to_sequencer".format(record.table)
        return Function(functionName, STAGE_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS trigger AS\n"
                         "$BODY$\n"
                         "BEGIN\n"
                         "  PERFORM {2}.{3}('{4}',new.id);\n"
                         "  RETURN new;\n"                                    
                         "END;\n"
                         "$BODY$\n"
                         "LANGUAGE plpgsql;\n\n").format(STAGE_SCHEMA, functionName, delegate.schema, delegate.name, record.table))
        
    def getStageTable(self, record):
        schema = self.specification.dedicatedStagingAreaName if self.specification.dedicatedStagingAreaName is not None else STAGE_SCHEMA
        tableName = record.getNativeTable()
        return Table(tableName, schema,
                    ("CREATE TABLE {0}.{1}(\n"
                    "  id bigint PRIMARY KEY DEFAULT nextval('shared.{2}_seq'){3},\n"
                    "  identification character varying(10) NOT NULL DEFAULT 'undefined',\n"
                    "  task_id integer not null DEFAULT 0\n"
                    "){4};\n\n").format(schema, tableName, record.table, 
                                        self._getColumnDefsSQL(filter(lambda field: field.dumpDataType != "geometry", record.getInputMappedFields())), 
                                        self._getTableParameters(False, record.withOids, None, None)))

    def getStageSequencerAddTrigger(self, record, table, triggerFunction):
        triggerName = "insert_of_stage_{0}".format(record.table)
        return Trigger(triggerName, table.name, triggerFunction.name, triggerFunction.schema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER INSERT ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, table.schema, table.name, triggerFunction.schema, triggerFunction.name))

    def getStageAdditionalStageIndex(self, index, record, table):
        indexName = "{0}_{1}_{2}".format(table.schema, record.table, index.underscoreDelimitedColumns)
        usingClause = "" if index.using is None else "USING {0} ".format(index.using) 
        return Index(indexName, table.name, table.schema,
                     "CREATE INDEX {0} ON {1}.{2} {3}({4});\n\n".format(indexName, table.schema, table.name, usingClause, index.commaDelimitedColumns))

    def getStageFunction(self, record):
        functionName = "stage_{0}".format(record.table) 
        nextIdFunctionName = "shared.get_next_{0}_id".format(record.table) 
        
        parameters = [SystemField("p_seq", "integer"), SystemField("p_task_id", "integer")]
        parameters += [SystemField("p_" + field.column, field.columnDataType) for field in record.getInputMappedFields()]
        parameters.append(SystemField("p_identification", "character varying"))
        
        return Function(functionName, STAGE_SCHEMA, 
                        [field.typeName for field in parameters],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}(\n"
                         "  {2}\n"
                         ") RETURNS SETOF shared.chimp_message AS $$\n"            
                         "DECLARE\n"
                         "  v_id bigint;\n"
                         "  v_message record;\n"
                         "BEGIN\n"
                         "  v_id = {3}();\n"
                         "  INSERT INTO {4}.{5} (\n"
                         "    id,\n"
                         "    task_id{6},\n"
                         "    identification)\n"        
                         "  VALUES (\n"
                         "    v_id,\n"
                         "    p_task_id{7},\n"
                         "    p_identification);\n"
                         "EXCEPTION\n"
                         "  WHEN others THEN\n"
                         "    v_message = shared.make_exception('STG001','Unhandled exception while staging row',NULL,1,SQLERRM);\n"
                         "    RETURN NEXT v_message;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(STAGE_SCHEMA, functionName,
                                                            "\n,  ".join([field.column + " " + field.typeName for field in parameters]), 
#                                                            self._getColumnDefsSQL(record.getInputMappedFields(), "p_", includeNotNulls = False),
                                                            nextIdFunctionName, STAGE_SCHEMA, record.table,
                                                            "".join(map(lambda field: ",\n    {0}".format(field.column), record.getInputMappedFields())),
                                                            "".join(map(lambda field: ",\n    p_{0}".format(field.column), record.getInputMappedFields()))))

    def getStageView(self, record, table):
        return View(record.table, STAGE_SCHEMA,
                    ("  CREATE OR REPLACE VIEW {0}.{1} AS\n"
                     "    SELECT *\n"
                     "    FROM {2}.{3};\n\n").format(STAGE_SCHEMA, record.table, table.schema, table.name))
        
    def getStageViewRule(self, record, table, view):
        ruleName = "{0}_delete".format(record.table)
        return Rule(ruleName, 
                    ("    CREATE RULE {0} AS ON DELETE TO {1}.{2}\n"
                     "        DO INSTEAD\n"
                     "        DELETE FROM {3}.{4}\n"
                     "         WHERE id = OLD.id;\n\n").format(ruleName, view.schema, view.name, table.schema, table.name))

    def getRedirectionFunction(self, dedicatedStagingAreaName, stageTable, finalTable, fromColumn, destinationColumn):
        functionName = "redirect_{0}_{1}".format(stageTable, fromColumn)         
        if fromColumn=="id":
            extra="  new.id = nextval('{0}.{1}_seq');\n".format(SHARED_SCHEMA, finalTable)
        else:
            extra=""            
        return Function(functionName, dedicatedStagingAreaName, 
                        [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"            
                         "BEGIN\n"
                         "  new.{2} = new.{3};\n{4}"
                         "  RETURN new;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(dedicatedStagingAreaName, 
                                                            functionName,
                                                            destinationColumn,
                                                            fromColumn,
                                                            extra))

    def getRedirectionTrigger(self, stageTable, triggerFunction):
        triggerName = "redirection_of_{0}_insert".format(stageTable)
        return Trigger(triggerName, stageTable, triggerFunction.name, triggerFunction.schema,
                       ("CREATE TRIGGER {0}\n"
                        "BEFORE INSERT ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, triggerFunction.schema, stageTable, triggerFunction.schema, triggerFunction.name))


    # MV
    def getMVUnmaterialisedView(self, entity, schemaName):
        viewName = "{0}_unmaterialized".format(entity.name)
        entityColumns = ""
        
        for table in entity.tables:
            entityColumns +=  "    {0}.id AS {1}_id,\n".format(table.alias, table.name)
            for column in table.columns:
                entityColumns += "    {0}.{1}{2},\n".format(table.alias, column.column, "" if column.alias is None else " AS {0}".format(column.alias))
                
        return View(viewName, MV_SCHEMA, 
                    ("CREATE OR REPLACE VIEW {0}.{1} AS\n"
                "  SELECT\n"
                "{2}"
                "    least({3}) AS visibility,\n"
                "    greatest({4}) AS security\n"
                "{5};\n\n").format(MV_SCHEMA, viewName, 
                                   entityColumns, 
                                   ", ".join(map(lambda table: "{0}.visibility".format(table.alias), entity.tables)),
                                   ", ".join(map(lambda table: "{0}.security".format(table.alias), entity.tables)),
                                   entity.getFullFromAndWhereClauses(schemaName, None, True)))

#
#    def getMVUnmaterialisedView(self, entity, schemaName):
#        viewName = "{0}_unmaterialized".format(entity.name)
#        modifiedColumnName = "mv_{0}_modified".format(entity.name)
#        entityColumns = ""
#        
#        for table in entity.tables:
#            entityColumns +=  "    {0}.id{1},\n".format(table.alias, "" if table.isLeadTable() else " AS {0}_id".format(table.name))
#            for column in table.columns:
#                entityColumns += "    {0}.{1}{2},\n".format(table.alias, column.column, "" if column.alias is None else " AS {0}".format(column.alias))
#        
#        return View(viewName, MV_SCHEMA, 
#                    ("CREATE OR REPLACE VIEW {0}.{1} AS\n"
#                "  SELECT\n"
#                "{2}"
#                "    greatest({3}) AS created,\n"
#                "    greatest({4}) AS {5},\n"
#                "    least({6}) AS visibility,\n"
#                "    greatest({7}) AS security\n"
#                "{8};\n\n").format(MV_SCHEMA, viewName, 
#                                   entityColumns, 
#                                   ", ".join(map(lambda table: "{0}.created".format(table.alias), entity.tables)),
#                                   ", ".join(map(lambda table: "{0}.{1}".format(table.alias, modifiedColumnName), entity.tables)),
#                                   modifiedColumnName,
#                                   ", ".join(map(lambda table: "{0}.visibility".format(table.alias), entity.tables)),
#                                   ", ".join(map(lambda table: "{0}.security".format(table.alias), entity.tables)),
#                                   entity.fromAndWhereClauses(schemaName, "   ", None)))
#

    def getMVSequence(self, entity):
        sequenceName = "{0}_seq".format(entity.name)
        return Sequence(sequenceName, MV_SCHEMA, 
                        ("CREATE SEQUENCE {0}.{1}\n"
                         "INCREMENT 1\n"
                         "MINVALUE 1\n"
                         "START 10\n"
                         "CACHE 1;\n\n").format(MV_SCHEMA, sequenceName))

#    
    def getMVTable(self, entity, view):
        tableColumns =  "    id bigint not null primary key default nextval('{0}.{1}_seq'),\n".format(MV_SCHEMA, entity.name)
        for table in entity.tables:
            tableColumns +=  "    {0}_id bigint,\n".format(table.name)
              
            for column in table.columns:
                columnLine = "    {0},\n".format(column.strippedColumnClause)
                tableColumns += columnLine

        
        for element in entity.computedData.elements:
            extraSystemFields = element.getExtraSystemFields()
            for extraSystemField in extraSystemFields:
#                sf = SystemField(extraSystemField["column"], extraSystemField["type"], size = extraSystemField["size"], nullable = extraSystemField["nullable"], default = extraSystemField["default"])
#                tableColumns += "    {0},\n".format(sf.getSQLFragment())                            
                tableColumns += "    {0},\n".format(extraSystemField.columnClause(None))

                
        tableColumns +=  "    last_refreshed timestamp with time zone NOT NULL default now(),\n"   
        tableColumns +=  "    visibility integer,\n"
        tableColumns +=  "    security integer\n"
                
        return Table(entity.name, MV_SCHEMA,
                     ("CREATE TABLE {0}.{1} (\n"
                      "{2}){3};\n\n").format(MV_SCHEMA, entity.name, tableColumns, self._getTableParameters(True, False, entity.fillFactor, entity.computedData.getDefaultFillFactor())))            

    
    def getMVTableIdIndex(self, entity, entityTable, mvTable):
        indexName = "{0}_{1}_id".format(entity.name, entityTable.name)
        columnName = "{0}_id".format(entityTable.name)
        return Index(indexName, mvTable.name, mvTable.schema, 
                     "CREATE INDEX {0} ON {1}.{2}({3});\n\n".format(indexName, mvTable.schema, mvTable.name, columnName))
        
    def getMVTableVisibilityIndex(self, entity, mvTable):
        indexName = "{0}_visibility".format(entity.name)
        return Index(indexName, mvTable.name, mvTable.schema,
                     "CREATE INDEX {0} ON {1}.{2}(visibility);\n\n".format(indexName, mvTable.schema, mvTable.name))        
    
    def getMVTableSecurityIndex(self, entity, mvTable):
        indexName = "{0}_security".format(entity.name)
        return Index(indexName, mvTable.name, mvTable.schema,
                     "CREATE INDEX {0} ON {1}.{2}(security);\n\n".format(indexName, mvTable.schema, mvTable.name))        
       
    def getMVTableAdditionalIndexIndex(self, index, entity, mvTable):
        indexName = "{0}_{1}".format(entity.name, index.underscoreDelimitedColumns)
        return Index(indexName, mvTable.name, mvTable.schema,
                     "CREATE INDEX {0} ON {1}.{2}{3}({4});\n\n".format(indexName, mvTable.schema, mvTable.name,
                                                                       "" if index.using is None else " USING {0} ".format(index.using),
                                                                       index.commaDelimitedColumns))
    def getMVDefaultVisibilityFunction(self, entity):
        functionName = "get_{0}_default_visibility".format(entity.name) 
        return Function(functionName, MV_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS integer AS $$\n"
                         "  BEGIN\n"    
                         "    RETURN {2};\n"
                         "  END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(MV_SCHEMA, functionName, entity.defaultVisibility))
    
    def getMVDefaultSecurityFunction(self, entity):
        functionName = "get_{0}_default_security".format(entity.name)
        return Function(functionName, MV_SCHEMA, [],
                        ("\nCREATE OR REPLACE FUNCTION {0}.{1}() RETURNS integer AS $$\n"
                         "  BEGIN\n"
                         "    RETURN {2};\n"
                         "  END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(MV_SCHEMA, functionName,  entity.defaultSecurity))

#    
#    def getMVRefreshRowFunction(self, entity, mvTable, unmaterializedView):
#        functionName = "{0}_refresh_row".format(entity.name)
#        return Function(functionName, MV_SCHEMA, ["bigint"],
#                        ("CREATE OR REPLACE FUNCTION {0}.{1}(p_id bigint) RETURNS void AS $$\n"
#                         "  BEGIN\n"    
#                         "    INSERT INTO {2}.{3}\n"
#                         "    SELECT *\n"
#                         "    FROM {4}.{5}\n"
#                         "    WHERE id=p_id;\n"
#                         "  END;\n"
#                         "$$ LANGUAGE plpgsql;\n\n").format(MV_SCHEMA, functionName, 
#                                                            mvTable.schema, mvTable.name,
#                                                            unmaterializedView.schema, unmaterializedView.name))             

    def getMVSearchDomainSourceRegistrationDML(self, entity):
        return DML(("SELECT search.register_domain_source('{0}', 'view', '{1}', '{2}', "
                   "'{3}', TRUE);\n\n").format(entity.search.searchDomain, MV_SCHEMA, entity.name, self.specification.name),
                   dropDdl="SELECT search.unregister_domain_source('{0}', '{1}', '{2}');\n".format(entity.search.searchDomain, MV_SCHEMA, entity.name))

    def getMVRefreshRowFunction(self, entity, mvTable, unmaterializedView):
        functionName = "refresh_{0}_row".format(entity.name)         
        tableColumns = []     
        for table in entity.tables:
            tableColumns.append("{0}_id".format(table.name))
                
            for column in table.columns:
                tableColumns.append(column.finalEntityColumn)            
        tableColumns.append("visibility")
        tableColumns.append("security")

        return Function(functionName, MV_SCHEMA, ["bigint", "boolean"],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}(p_{2}_id bigint, p_delete_required boolean) RETURNS void AS $$\n"
                         "  BEGIN\n"
                         "    IF p_delete_required THEN\n"        
                         "      DELETE FROM {3}.{4}\n"
                         "      WHERE {2}_id = p_{2}_id;\n"
                         "    END IF;\n\n"  
                         "    INSERT INTO {3}.{4} ({5})\n"
                         "    SELECT {5}\n"
                         "    FROM {6}.{7} AS a\n"
                         "    WHERE a.{2}_id = p_{2}_id;\n"
                         "  END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(MV_SCHEMA, functionName, entity.tables[0].name,
                                                            mvTable.schema,  mvTable.name, ",    \n".join(tableColumns),
                                                            unmaterializedView.schema, unmaterializedView.name))             



  

    # AFTER INSERT
    def getMVEntityInsertTriggerFunction(self, table, entity, mvTable, refreshRowFunction, schemaName):

        def getWhere(prefix):
            joins = []
            for join in table.joins:
                joins.append("{0} = {1}.{2}".format(join.foreignColumn, prefix, join.column))
            return(" AND ".join(joins))

        
        functionName = "insert_of_{0}_by_{1}".format(entity.name, table.name)
                
        if table.joinType is None:                
            body = ("    -- This is the 'lead' table\n"
                    "    PERFORM {0}.refresh_{1}_row (new.id, False);\n").format(MV_SCHEMA, entity.name)
        else:
            if table.joinType=="inner":
                body  = "    -- This table is inner-joined\n"
            elif table.joinType=="left":
                body  = "    -- This table is left joined\n"
                
            body += ("    PERFORM {0}.refresh_{1}_row (a.{2}_id, False)\n"
                     "    FROM {0}.{1}_unmaterialized AS a\n"
                     "    WHERE {3};\n\n").format(MV_SCHEMA, entity.name, entity.tables[0].name, getWhere("new"))
                                                                                    
        return Function(functionName, MV_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
                         "  BEGIN\n{2}"  
                         "    RETURN null;\n"                
                         "  END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(MV_SCHEMA, functionName, body))
    

    def getMVEntityInsertTrigger(self, table, entity, schemaName, triggerFunction):
        storageTableName = table.name
        triggerName = "c_mv_insert_of_{0}_by_{1}".format(entity.name, table.name)
                    
        return Trigger(triggerName, storageTableName, triggerFunction.name, schemaName, 
                       ("CREATE TRIGGER {0} AFTER INSERT ON {1}.{2}\n"
                        "  FOR EACH ROW EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, schemaName, storageTableName, triggerFunction.schema, triggerFunction.name))

    
    # AFTER UPDATE
    def getMVEntityUpdateTriggerFunction(self, table, entity, mvTable, refreshRowFunction, schemaName):
        def getWhere(prefix):
            joins = []                    
            for join in table.joins:           
                joins.append("{0} = {1}.{2}".format(join.finalForeignColumn, prefix, join.column))
            return(" AND ".join(joins))


        def getJoinChangeCondition():
            joins = []                    
            for join in table.joins:                
                joins.append("(old.{0} IS DISTINCT FROM new.{0})".format(join.column, join.column))
            return(" AND ".join(joins))

        functionName = "update_of_{0}_by_{1}".format(entity.name, table.name)
                
        if table.joinType is None:                
            body = ("    -- This is the 'lead' table\n"
                    "    PERFORM {0}.refresh_{1}_row (new.id, True);\n").format(MV_SCHEMA, entity.name)
        else:
            if table.joinType=="inner":
                body  = "    -- This table is inner-joined\n"
            elif table.joinType=="left":
                body  = "    -- This table is left joined\n"
                
            body += ("    PERFORM {0}.refresh_{1}_row (a.{2}_id, True)\n"
                     "    FROM {0}.{1} AS a\n"
                     "    WHERE {3};\n\n").format(MV_SCHEMA, entity.name, entity.tables[0].name, getWhere("old"))

            body += ("    IF {0} THEN\n"
                     "      PERFORM {1}.refresh_{2}_row (a.{3}_id, True)\n"
                     "      FROM {1}.{2}_unmaterialized AS a\n"
                     "      WHERE {4};\n"
                     "    END IF;\n\n").format(getJoinChangeCondition(), MV_SCHEMA, entity.name, entity.tables[0].name, getWhere("new"))
                                                                                    
        return Function(functionName, MV_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
                         "  BEGIN\n{2}"  
                         "    RETURN null;\n"                
                         "  END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(MV_SCHEMA, functionName, body))

                         
    def getMVEntityUpdateTrigger(self, specificationName, table, entity, schemaName, triggerFunction):
        storageTableName = table.name
        triggerName = "c_mv_update_of_{0}_by_{1}".format(entity.name, table.name)
        triggeringColumns = ["visibility", "security"]

        #for join in table.joins:
        #    triggeringColumns.append(join.column)
                
        allTriggeringFields = entity.getAllFields(specificationName, None, False, None, table.name, returnSourceColumnName=True)
        for field in allTriggeringFields:
            triggeringColumns.append(field.column)


        if len(table.additionalTriggeringColumns)>0:
            for column in table.additionalTriggeringColumns:
                triggeringColumns.append(column)
                
        triggeringColumns = set(triggeringColumns)
        
        return Trigger(triggerName, storageTableName, triggerFunction.name, schemaName, 
                       ("CREATE TRIGGER {0} AFTER UPDATE OF {1} ON {2}.{3}\n"
                        "  FOR EACH ROW WHEN ({4}) EXECUTE PROCEDURE {5}.{6}();\n"
                        "\n").format(triggerName, ", ".join(triggeringColumns), schemaName, storageTableName,
                                     " OR ".join(map(lambda col: "old.{0} IS DISTINCT FROM new.{0}".format(col), triggeringColumns)),
                                     triggerFunction.schema, triggerFunction.name))
    

    
    
    def getMVEnableTriggersDDL(self, entity, schemaName):
        return self._getMVSetTriggersEnabledDDL(entity, schemaName, True)
    
    def getMVDisableTriggersDDL(self, entity, schemaName):
        return self._getMVSetTriggersEnabledDDL(entity, schemaName, False)
    
    def _getMVSetTriggersEnabledDDL(self, entity, schemaName, enabled):
        lines = []
        enable = "ENABLE" if enabled else "DISABLE"
        
        for table in entity.tables:
            # lines.append("ALTER TABLE {0}.{1} {2} TRIGGER b_{1}_mv_{3}_update;\n".format(schemaName, table.name, enable, entity.name))
            lines.append("ALTER TABLE {0}.{1} {2} TRIGGER c_mv_insert_of_{3}_by_{1};\n".format(schemaName, table.name, enable, entity.name))
            lines.append("ALTER TABLE {0}.{1} {2} TRIGGER c_mv_update_of_{3}_by_{1};\n".format(schemaName, table.name, enable, entity.name, table.name))
        
#        for thisFindRecord in self.specification.records:
#            for thisAction in thisFindRecord.mvModifyingActions:
#                if thisAction.entityName == entity.name:
#                    lines.append("ALTER TABLE {0}.{1} {2} TRIGGER d_modify_{3}_mv_{4}_from_{1}_insert;\n".format(schemaName, thisFindRecord.table, enable, thisAction.targetTable, entity.name))                            
#                    lines.append("ALTER TABLE {0}.{1} {2} TRIGGER d_modify_{3}_mv_{4}_from_{1}_update;\n".format(schemaName, thisFindRecord.table, enable, thisAction.targetTable, entity.name))
                
        return DML("".join(lines));
    
    def getMVOnRecreateTriggersDML(self, entity):
        
        tableColumns = []     
        for table in entity.tables:
            tableColumns.append("{0}_id".format(table.name))
                
            for column in table.columns:
                tableColumns.append(column.finalEntityColumn)
                            
        tableColumns.append("visibility")
        tableColumns.append("security")

        
        return DML(("TRUNCATE TABLE {0}.{1};\n"
                    "INSERT INTO {0}.{1} ({2}) SELECT {2} FROM {0}.{1}_unmaterialized;\n").format(MV_SCHEMA, entity.name, ", ".join(tableColumns)))
    

        
                
    # (util)
    def _getColumnDefsSQL(self, fields, prefix = "", indent = 2, includeNotNulls = True):
        return "".join(map(lambda field: ",\n  {0}{1}".format(prefix, field.strippedColumnClause(None, includeNotNulls)), fields))
        
    def _getSystemColumnDefsSQL(self, systemFields, prefix = ""):
        return "".join(map(lambda field: ",\n  {0}{1}".format(prefix, field.getSQLFragment()), systemFields))
        
    def _getTableParameters(self, finalDestination, withOids, fillFactor, defaultFillFactor):        
        if withOids or ((fillFactor is not None or defaultFillFactor is not None) and finalDestination):
            params = []
            if (fillFactor is not None or defaultFillFactor is not None) and finalDestination:
                if fillFactor is None:
                    fillFactor = defaultFillFactor
                params.append("FILLFACTOR={0}".format(fillFactor))
            if withOids:
                params.append("OIDS=TRUE")                
            r = "\nWITH ({0})".format(",".join(params))
        else:
            r = ""        
        return r
                          
    def _getFinalDestinationSystemFields(self, record):
        info = []
        for element in record.computedData.elements:
            extraSystemFields = element.getExtraSystemFields()
            for sf in extraSystemFields:    
                if sf.columnDataType == "character varying":
                    textSize = sf.size
                else:
                    textSize = None
                info.append(SystemField(sf.column, sf.columnDataType, size=textSize, nullable = (not sf.mandatory), default = sf.default))
        info.append(SystemField("visibility", "integer", nullable = True))
        info.append(SystemField("security", "integer", nullable = True))
        return info
    
    
    def getStorageTable(self, finalDestination, record, schemaName):
        return Table(record.table, schemaName, 
                     ("CREATE TABLE {0}.{1}(\n"
                        "  id bigint PRIMARY KEY{2}{3},\n"
                        "  created timestamp with time zone NOT NULL DEFAULT now(),\n"                        
                        "  modified timestamp with time zone NOT NULL DEFAULT now(){4}{5}\n"
                        "){6};\n\n").format(schemaName, record.table,
                                            "" if schemaName != "editable" else self._getSystemColumnDefsSQL(EDITABLE_SYSTEM_FIELDS),
                                            self._getColumnDefsSQL(record.getAllMappedFields()),
                                            "" if schemaName != "import" else self._getSystemColumnDefsSQL(IMPORT_SYSTEM_FIELDS),
                                            "" if record.getDestinationTargetSchema() != schemaName else self._getSystemColumnDefsSQL(self._getFinalDestinationSystemFields(record)),
                                            self._getTableParameters(finalDestination, record.withOids, record.fillFactor, record.computedData.getDefaultFillFactor())))

    
    def getStorageHistoryTable(self, record, schemaName, storageTable):
        tableName = "{0}_{1}_changes".format(schemaName, record.table)        
        return Table(tableName, HISTORY_SCHEMA,
                     ("CREATE TABLE {0}.{1} AS\n"
                      "SELECT *\n"
                      "FROM {2}.{3};\n"
                      "\n"
                      "ALTER TABLE {0}.{1} ADD COLUMN valid_until timestamp with time zone NOT NULL DEFAULT now();\n"
                      "\n").format(HISTORY_SCHEMA, tableName, storageTable.schema, storageTable.name))

    
    def getStorageHistoryChangesPrimaryKeyIndex(self, record, schemaName, historyTable):
        indexName = "{0}_{1}_changes_pk".format(schemaName, record.table)
        return Index(indexName, historyTable.name, historyTable.schema,
                     "CREATE INDEX {0} ON {1}.{2} ({3});\n".format(indexName, historyTable.schema, historyTable.name, 
                                                                   ", ".join(record.primaryKeyColumns)))
    
    def getStorageHistoryChangesValidUntilIndex(self, record, schemaName, historyTable):
        indexName = "{0}_{1}_changes_valid_until".format(schemaName, record.table)
        return Index(indexName, historyTable.name, historyTable.schema,
                     "CREATE INDEX {0} ON {1}.{2} (valid_until);\n".format(indexName, historyTable.schema, historyTable.name))

    
    def getStorageHistoryProcessor(self, functionName, record, schemaName, historyTable, preInsertSql=""):               
        return Function(functionName, schemaName, [],
                ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
                 "BEGIN{2}\n"
                 "  INSERT INTO {3}.{4}(\n"                
                "    id{5}{6},\n"                
                "    created,\n"
                "    modified{7}\n"                
                ")\n"
                "  VALUES (\n"                
                "    old.id{8}{9},\n"
                "    old.created,\n"
                "    old.modified{10}\n"                
                "  );\n"
                "  RETURN new;\n"
                "END;\n"
                "$$ LANGUAGE plpgsql;\n\n").format(schemaName, functionName,
                   preInsertSql,
                   historyTable.schema, historyTable.name,
                   "" if schemaName != "editable" else "".join(map(lambda field: ",\n    {0}".format(field.column), EDITABLE_SYSTEM_FIELDS)),
                   "".join(map(",\n    {0}".format, self._getStorageTriggeringColumns(record, schemaName))),
                   "" if schemaName != "import" else "".join(map(lambda field: ",\n    {0}".format(field.column), IMPORT_SYSTEM_FIELDS)),
                   "" if schemaName != "editable" else "".join(map(lambda field: ",\n    old.{0}".format(field.column), EDITABLE_SYSTEM_FIELDS)),
                   "".join(map(",\n    old.{0}".format, self._getStorageTriggeringColumns(record, schemaName))),
                   "" if schemaName != "import" else "".join(map(lambda field: ",\n    old.{0}".format(field.column), IMPORT_SYSTEM_FIELDS))))

    
    def getStorageHistoryChangeProcessorFunction(self, record, schemaName, historyTable):
        functionName = "{0}_change_processor".format(record.table)        
        preInsertSql = "\n  new.modified = now();{0}".format("" if schemaName != "import" else "\n  new.modified_task_id = new.last_affirmed_task_id;")
        return self.getStorageHistoryProcessor(functionName, record, schemaName, historyTable, preInsertSql)
       
    
    def getStorageHistoryChangeProcessorTrigger(self, record, schemaName, storageTable, changeProcessorFunction):
        triggerName = "{0}_change_processor".format(record.table)
        return Trigger(triggerName, storageTable.name, changeProcessorFunction.name, storageTable.schema, 
                       ("CREATE TRIGGER {0} BEFORE UPDATE OF {1}\n"
                        "ON {2}.{3} FOR EACH ROW\n"        
                        "WHEN ({4})\n" 
                        "EXECUTE PROCEDURE {5}.{6}();\n\n").format(triggerName, ", ".join(self._getStorageTriggeringColumns(record, schemaName)),
                                                                   schemaName, record.table,
                                                                   " OR ".join(map("old.{0} IS DISTINCT FROM new.{0}".format, self._getStorageTriggeringColumns(record, schemaName))),
                                                                   changeProcessorFunction.schema, changeProcessorFunction.name))
        
    def _getStorageTriggeringColumns(self, record, schemaName):
        triggeringColumns = list(map(lambda field: field.column, record.getAllMappedFields()))
        if schemaName == record.getDestinationTargetSchema():
            triggeringColumns.append("visibility")
            triggeringColumns.append("security")
        return triggeringColumns
    
    def _getSearchTriggeringColumns(self, record, schemaName):
        triggeringColumns = []
        for thisAttribute in record.search.attributes:
            triggeringColumns.append(thisAttribute.column)
        triggeringColumns.append("visibility")
        triggeringColumns.append("security")
        return triggeringColumns
    
    def getStorageHistoryDeletesTable(self, record, schemaName, storageTable):
        tableName = "{0}_{1}_deletes".format(schemaName, record.table)        
        return Table(tableName, HISTORY_SCHEMA, 
                     ("CREATE TABLE {0}.{1} AS\n"
                      "SELECT *\n"
                      "FROM {2}.{3};\n"
                      "\n"
                      "ALTER TABLE {0}.{1} ADD COLUMN deleted timestamp with time zone NOT NULL DEFAULT now();\n"
                      "\n").format(HISTORY_SCHEMA, tableName, 
                                   storageTable.schema, storageTable.name))
    
    def getStorageHistoryDeletesPrimaryKeyIndex(self, record, schemaName, historyTable):
        indexName = "{0}_{1}_deletes_pk".format(schemaName, record.table)
        return Index(indexName, historyTable.name, historyTable.schema, 
                     "CREATE INDEX {0} ON {1}.{2} ({3});\n".format(indexName, historyTable.schema, historyTable.name, ", ".join(record.primaryKeyColumns)))

    def getStorageHistoryDeletesDeletedIndex(self, record, schemaName, historyDeletesTable):
        indexName = "{0}_{1}_deletes_deleted".format(schemaName, record.table)
        return Index(indexName, historyDeletesTable.name, historyDeletesTable.schema,
                     "CREATE INDEX {0} ON {1}.{2} (deleted);\n".format(indexName, historyDeletesTable.schema, historyDeletesTable.name))

    
    def getStorageHistoryDeleteProcessorFunction(self, record, schemaName, historyDeletesTable):
        return self.getStorageHistoryProcessor("{0}_delete_processor".format(record.table), record, schemaName, historyDeletesTable)

    
    def getStorageHistoryDeleteProcessorTrigger(self, record, schemaName, storageTable, deleteProcessorFunction):
        triggerName = "{0}_delete_processor".format(record.table)
        return Trigger(triggerName, storageTable.name, deleteProcessorFunction.name, storageTable.schema, 
                       ("CREATE TRIGGER {0} AFTER DELETE\n"
                        "ON {1}.{2} FOR EACH ROW\n"            
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, storageTable.schema, storageTable.name,
                                                                 deleteProcessorFunction.schema, deleteProcessorFunction.name))                        

    def _getConstantFunction(self, schema, name, value, dataType = "smallint"):
        return Function(name, schema, [],
            ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS {2} AS $$\n"
             "  BEGIN\n"
             "    RETURN {3};\n" 
             "  END;\n"
             "$$ LANGUAGE plpgsql;  \n\n").format(schema, name, dataType, value))
            
    def getStorageDefaultVisibilityFunction(self, record, schemaName):
        return self._getConstantFunction(schemaName, "get_{0}_default_visibility".format(record.table), record.defaultVisibility)
            
    def getStorageDefaultSecurityFunction(self, record, schemaName):
        return self._getConstantFunction(schemaName, "get_{0}_default_security".format(record.table), record.defaultSecurity)
    
    def getPinChangeProcessorFunction(self, record, pin, schemaName):
        functionName = "{0}_{1}_pin_change_processor".format(record.table, pin.name) 
        return Function(functionName, schemaName, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
                         "BEGIN\n"
                         "  new.{2} = now();\n"
                         "  RETURN new;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(schemaName, functionName, "{0}_pin_modified".format(pin.name)))                     

    def getPinChangeProcessorTrigger(self, record, pin, schemaName, storageTable, pinChangeProcessor):
        triggerName = "{0}_{1}_pin_change_processor".format(record.table, pin.name)
        return Trigger(triggerName, storageTable.name, pinChangeProcessor.name, storageTable.schema, 
                       ("CREATE TRIGGER {0} AFTER UPDATE OF {1}, {2}, visibility, security\n" 
                        "ON {3}.{4} FOR EACH ROW\n"
                        "WHEN (old.visibility IS DISTINCT FROM new.visibility OR old.security IS DISTINCT FROM new.security OR old.{1} IS DISTINCT FROM new.{1} OR old.{2} IS DISTINCT FROM new.{2})\n"
                        "EXECUTE PROCEDURE {5}.{6}();\n\n").format(triggerName, pin.xColumn, pin.yColumn,
                                                                   storageTable.schema, storageTable.name,
                                                                   pinChangeProcessor.schema, pinChangeProcessor.name))                    

    
    def getSearchChangeProcessorFunction(self, record, schemaName):
        functionName = "{0}_search_change_processor".format(record.table)
        return Function(functionName, schemaName, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
                        "BEGIN\n"
                        "  new.search_modified = now();\n"
                        "  RETURN new;\n"
                        "END;\n"
                        "$$ LANGUAGE plpgsql;\n\n").format(schemaName, functionName))

    
    def getSearchChangeProcessorTrigger(self, record, schemaName, storageTable, searchChangeProcessor):
        triggerName = "{0}_search_change_processor".format(record.table)
        return Trigger(triggerName, storageTable.name, searchChangeProcessor.name, storageTable.schema, 
           ("CREATE TRIGGER {0} AFTER UPDATE OF {1}\n" 
            "ON {2}.{3} FOR EACH ROW\n"
            "WHEN ({4})\n"
            "EXECUTE PROCEDURE {5}.{6}();\n\n").format(triggerName, 
               ", ".join(self._getSearchTriggeringColumns(record, schemaName)),
               storageTable.schema, storageTable.name,
               " OR ".join(map("old.{0} IS DISTINCT FROM new.{0}".format, self._getSearchTriggeringColumns(record, schemaName))),
               searchChangeProcessor.schema, searchChangeProcessor.name))

#    
#    def getStorageLogicalDeleteRule(self, record, schemaName, storageTable):
#        ruleName = "{0}_logical_delete".format(record.table)
#        return Rule(ruleName, ("CREATE OR REPLACE RULE {0} AS\n" 
#                            "ON DELETE TO {1}.{2} DO INSTEAD \n"
#                            "UPDATE {1}.{2} SET visibility=10\n"
#                            "WHERE {3}.id=old.id;\n\n").format(ruleName, storageTable.schema, storageTable.name,
#                                                               record.table))                   

    # Before 
#    def getMVModifiedUpdateTriggerFunction(self, table, entity, schemaName):
#        functionName = "b_{0}_mv_{1}_modified_update".format(table.name, entity.name)
#        lastModifiedColumn = "mv_{0}_modified".format(entity.name)
#
#
#        leadTableRefresh =""        
#        if len(table.leadTableRefreshTriggeringColumns)>0:
#            leadTableRefresh += "    -- Possible this update will not be reflected in modified timestamp\n"
#            leadTableRefresh += "    -- due to an outer join... explicitly set modified for 'leading' table\n"
#            leadTable = entity.tables[0].name
#            leadTableRefresh += "    UPDATE {0}.{1}\n".format(schemaName, leadTable)
#            leadTableRefresh += "    SET mv_{0}_modified = now()\n".format(entity.name)
#            leadTableRefresh += "    WHERE id IN (SELECT {0}_id FROM mv.{1} WHERE {2}_id = new.id);\n\n".format(leadTable, entity.name, table.name)
#
#
#        
#        return Function(functionName, schemaName, [],
#                        ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
#                        "  BEGIN\n"   
#                        "    new.{2} = now();\n"
#                        "    RETURN new;\n"
#                        "  END;\n"
#                        "$$ LANGUAGE plpgsql;\n\n").format(schemaName, functionName, lastModifiedColumn))               

    
    def getMVModifiedUpdateTrigger(self, table, entity, schemaName, storageTable, modifiedUpdateFunction):
        triggerName = "b_{0}_mv_{1}_update".format(table.name, entity.name)
                                
        triggeringColumns = []
        for thisColumn in table.columns:
            triggeringColumns.append(thisColumn.column)
        for thisColumn in table.additionalTriggeringColumns:
            triggeringColumns.append(thisColumn)
        triggeringColumns.append("visibility")
        triggeringColumns.append("security")

        return Trigger(triggerName, storageTable.name, modifiedUpdateFunction.name, storageTable.schema,
                       ("CREATE TRIGGER {0} BEFORE UPDATE OF {1} ON {2}.{3}\n"
                        "  FOR EACH ROW WHEN ({4}) EXECUTE PROCEDURE {5}.{6}();\n"
                        "\n").format(triggerName, ", ".join(triggeringColumns), storageTable.schema, storageTable.name,
                                     " OR ".join(map("old.{0} IS DISTINCT FROM new.{0}".format, triggeringColumns)),
                                     modifiedUpdateFunction.schema, modifiedUpdateFunction.name))
    
#    def getMVUpdateTimestampFunction(self, action, record, schemaName):
#        functionName = "set_{0}_mv_{1}_modified_to_now".format(action.targetTable, action.entityName)
#        targetTableName = action.targetTable
#        mvModifiedColumnName = "mv_{0}_modified".format(action.entityName)
#        
#        return Function(functionName, schemaName, 
#                map(lambda join: join.localField.columnDataType, action.joins),
#                ("CREATE OR REPLACE FUNCTION {0}.{1} ("                
#                "  {2}\n"
#                ") RETURNS void AS $$\n"
#                "BEGIN\n"
#                "  UPDATE {3}.{4}\n"
#                "  SET {5} = current_timestamp\n"
#                "  WHERE\n"
#                "    {6};\n"
#                "END;\n"    
#                "$$ LANGUAGE plpgsql;\n\n").format(
#                        schemaName, functionName,
#                        ", ".join(map(lambda join: "p_{0} {1}".format(join.localColumn, join.localField.columnDataType), action.joins)),
#                        schemaName, targetTableName,
#                        mvModifiedColumnName,
#                        " AND ".join(map(lambda join: "{0} = p_{0}".format(join.localColumn), action.joins))))

    
#    def getMVOnInsertTriggerFunction(self, action, record, schemaName, updateTimestampFunction):
#        functionName = "set_mv_{0}_modified_on_{1}_from_{2}_insert".format(action.entityName, action.targetTable, record.table)
#        return Function(functionName, schemaName, [],
#                ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
#                 "BEGIN\n"
#                 "  PERFORM {2}.{3}({4});\n"              
#                 "  RETURN new;\n"
#                 "END;\n"
#                 "$$ LANGUAGE plpgsql;\n\n").format(
#                            schemaName, functionName,
#                            updateTimestampFunction.schema, updateTimestampFunction.name,
#                            ", ".join(map(lambda join: "new.{0}".format(join.localColumn), action.joins))))
#
#    
#    def getMVOnInsertTrigger(self, action, record, schemaName, storageTable, onInsertFunction):
#        triggerName = "d_modify_{0}_mv_{1}_from_{2}_insert".format(action.targetTable, action.entityName,record.table)
#        return Trigger(triggerName, storageTable.name, onInsertFunction.name, storageTable.schema, 
#               ("CREATE TRIGGER {0}\n"
#                "AFTER INSERT ON {1}.{2}\n"
#                "FOR EACH ROW\n"
#                "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, 
#                                                           storageTable.schema, storageTable.name,
#                                                           onInsertFunction.schema, onInsertFunction.name))                        

    
#    def getMVOnUpdateTriggerFunction(self, action, record, schemaName, updateTimestampFunction):
#        functionName = "set_mv_{0}_modified_on_{1}_from_{2}_update".format(action.entityName, action.targetTable, record.table)
#        return Function(functionName, schemaName, [],
#                ("CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS trigger AS $$\n"
#                "BEGIN\n"
#                "  PERFORM {2}.{3}({4});\n"
#                "  IF {5} THEN\n"
#                "    PERFORM {2}.{3}({6});\n"
#                "  END IF;\n"
#                "  RETURN new;\n"
#                "END;\n"
#                "$$ LANGUAGE plpgsql;\n\n").format(schemaName, functionName,
#                                                   updateTimestampFunction.schema, updateTimestampFunction.name,
#                                                   ", ".join(map(lambda join: "new.{0}".format(join.localColumn), action.joins)),
#                                                   " OR ".join(map(lambda join: "shared.different(old.{0}, new.{0})".format(join.localColumn), action.joins)),
#                                                   ", ".join(map(lambda join: "old.{0}".format(join.localColumn), action.joins))))
#
#    
#    def getMVOnUpdateTrigger(self, action, record, schemaName, storageTable, onUpdateFunction):
#        triggerName = "d_modify_{0}_mv_{1}_from_{2}_update".format(action.targetTable, action.entityName,record.table)
#        return Trigger(triggerName, storageTable.name, onUpdateFunction.name, storageTable.schema, 
#                       ("CREATE TRIGGER {0}\n"
#                        "AFTER UPDATE ON {1}.{2}\n"
#                        "FOR EACH ROW\n"
#                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, 
#                                                                   storageTable.schema, storageTable.name,
#                                                                   onUpdateFunction.schema, onUpdateFunction.name))
    
    def createStorageSpecificationPrimaryKeyIndex(self, record, schemaName, storageTable):
        indexName = "{0}_{1}_specification_pk".format(schemaName, record.table)
        return Index(indexName, storageTable.name, storageTable.schema, 
                     "CREATE UNIQUE INDEX {0} ON {1}.{2} ({3});\n".format(indexName, storageTable.schema, storageTable.name,
                                                                          ", ".join(record.primaryKeyColumns)))

    
    def createStorageCreatedIndex(self, record, schemaName, storageTable):
        indexName = "{0}_{1}_created".format(schemaName, record.table)
        return Index(indexName, storageTable.name, storageTable.schema,
                     "CREATE INDEX {0} ON {1}.{2} (created);\n".format(indexName, storageTable.schema, storageTable.name))

    def createStorageModifiedIndex(self, record, schemaName, storageTable):
        indexName = "{0}_{1}_modified".format(schemaName, record.table)
        return Index(indexName, storageTable.name, storageTable.schema,
                     "CREATE INDEX {0} ON {1}.{2} (modified);\n".format(indexName, storageTable.schema, storageTable.name))

    
    def createStorageLastAffirmedTaskIndex(self, record, schemaName, storageTable):
        indexName = "{0}_{1}_last_affirmed_task_id".format(schemaName, record.table)
        return Index(indexName, storageTable.name, storageTable.schema,
                     "CREATE INDEX {0} ON {1}.{2} (last_affirmed_task_id);\n".format(indexName, storageTable.schema, storageTable.name))   

    
    def createStorageAdditionalIndex(self, index, record, schemaName, storageTable):
        indexName = "{0}_{1}_{2}".format(schemaName, record.table, index.underscoreDelimitedColumns)
        return Index(indexName, storageTable.name, storageTable.schema,
                     "CREATE INDEX {0} ON {1}.{2} {3}({4});\n".format(indexName, storageTable.schema, storageTable.name,
                                                                      "" if index.using is None else "USING {0}".format(index.using),
                                                                      index.commaDelimitedColumns))

    def createStoragePinModifiedIndex(self, pin, record, schemaName, storageTable):
        columnName = "{0}_pin_modified".format(pin.name)
        indexName = columnName
        return Index(indexName, storageTable.name, storageTable.schema,
                     "CREATE INDEX {0} ON {1}.{2} ({3});\n".format(indexName, storageTable.schema, storageTable.name, columnName))

    def createStorageEntityTimestampIndex(self, table, entity, record, schemaName, storageTable):
        columnName = "mv_{0}_modified".format(entity.name)
        indexName = "{0}_{1}_modified".format(table.name, entity.name)
        return Index(indexName, storageTable.name, storageTable.schema,
                     "CREATE INDEX {0} ON {1}.{2}({3});\n".format(indexName, storageTable.schema, storageTable.name, columnName))

    def getAreaWithinFunction(self, record, schemaName):
        functionName = "get_points_{0}".format(record.areaName) 
        return Function(functionName, AREA_SCHEMA, ['geometry'],
                        ("\nCREATE OR REPLACE FUNCTION {0}.{1}(p_point geometry) RETURNS areas.area_info AS $$\n"
                         "DECLARE\n"
                         "  r areas.area_info;\n" 
                         "BEGIN\n"
                         "  r.area_name='{2}';\n"   
                         "  SELECT {3}::character varying, {4}::character varying\n"
                         "  INTO r.polygon_label, r.polygon_id\n"
                         "  FROM {7}.{5}\n"
                         "  WHERE ST_Within(p_point, {6});\n"
                         "  RETURN r;\n"
                         "END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(AREA_SCHEMA, functionName, record.areaName, record.areaLabelColumn, record.areaIdColumn, record.table, record.areaGeometryColumn, schemaName))

    
    def getStageValidFunction(self, record):
        functionName = "{0}_valid".format(record.table)
        return Function(functionName, STAGE_SCHEMA,
                        map(lambda field: field.columnDataType, record.getInputMappedFields()),
                        ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
                        "  {2}\n"         
                        ") RETURNS SETOF shared.chimp_message AS $$\n"
                        "DECLARE\n"       
                        "  v_message RECORD;\n"
                        "BEGIN\n"       
                        "  --\n"
                        "  -- Example:\n"
                        "  -- \n"
                        "  -- IF p_my_column IS NULL THEN\n"
                        "  --   v_record.level = 'error';\n"
                        "  --   v_record.code = '1977';\n"
                        "  --   v_record.title = 'Value for ''My column'' must be specified';\n"
                        "  --   v_record.affected_columns = 'my_column';\n"
                        "  --   v_record.affected_row_count = 1;\n"
                        "  --   v_record.content = 'blah blah';\n"
                        "  --\n"
                        "  --   RETURN NEXT v_record;\n"
                        "  -- END IF;\n"
                        "  --\n\n"
                        "END;\n"
                        "$$ LANGUAGE plpgsql;\n\n").format(STAGE_SCHEMA, functionName,
                                                           ",\n  ".join(map(lambda field: "p_{0} {1}".format(field.column, field.columnDataType), record.getInputMappedFields()))))

    
#    def getStorageValidFunction(self, record, schemaName):
#        functionName = "{0}_valid".format(record.table)
#        delegateFunctionName = "{0}_valid".format(record.table)
#
#        if (schemaName == "import"):
#            forLoop = ("  FOR v_message IN SELECT * FROM {0}.{1} (\n"
#                       "    {2}").format(STAGE_SCHEMA, delegateFunctionName, 
#                                         ",\n    ".join(["p_" + field.column for field in record.getInputMappedFields()]))
#        else:
#            forLoop = ("  FOR v_message IN SELECT * FROM {0}.{1} (\n"
#                       "    {2}").format(IMPORT_SCHEMA, delegateFunctionName,
#                                         ",\n    ".join(["p_" + field.column for field in record.getAllMappedFields()]))
#
#        return Function(functionName, schemaName,
#                map(lambda field: field.columnDataType, record.getAllMappedFields()),
#                ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
#                "  {2}\n"
#                ") RETURNS SETOF shared.chimp_message AS $$\n"
#                "DECLARE\n"       
#                "  v_message RECORD;\n"
#                "BEGIN\n"
#                "  --\n"
#                "  -- Example:\n"
#                "  -- \n"
#                "  -- IF p_my_column IS NULL THEN\n"
#                "  --   v_record.level = 'error';\n"
#                "  --   v_record.code = '1977';\n"
#                "  --   v_record.title = 'Value for ''My column'' must be specified';\n"
#                "  --   v_record.affected_columns = 'my_column';\n"
#                "  --   v_record.affected_row_count = 1;\n"
#                "  --   v_record.content = 'blah blah';\n"
#                "  --\n"
#                "  --   RETURN NEXT v_record;\n"
#                "  -- END IF;\n"
#                "  --\n\n"
#                "  --Check if passes import validation\n"
#                "{3}"
#                ") LOOP\n"
#                "    RETURN NEXT v_message;\n"
#                "  END LOOP;\n\n"
#                "END;\n"
#                "$$ LANGUAGE plpgsql;\n\n").format(schemaName, functionName,
#                                                   ",\n  ".join(map(lambda field: "p_{0} {1}".format(field.column, field.columnDataType), record.getAllMappedFields())),
#                                                   forLoop))
#    
#    def getStorageDeletableFunction(self, record, schemaName, storageTable):
#        functionName = "{0}_deletable".format(record.table)   
#        
#        if schemaName == "editable":
#            primaryKeyColumns = [SystemField("id", "bigint")]
#        elif not record.hasPrimaryKey():
#            primaryKeyColumns = [SystemField("id", "bigint")]
#        elif len(record.primaryKeyColumns)==1 and record.primaryKeyColumns[0]=="id":
#            primaryKeyColumns = [SystemField("id", "bigint")]                        
#        else:
#            primaryKeyColumns = record.getPrimaryKeyFields()
#            
#             
##        primaryKeyColumns = [SystemField("id", "bigint")] if (schemaName == "editable" or not record.hasPrimaryKey()) else record.getPrimaryKeyFields()              
#        
#        
#        body = ("SELECT exists(select true from {0}.{1} where {2}"             
#                ")\n"
#                "    INTO v_row_exists;\n"
#                "    IF NOT v_row_exists THEN\n"                
#                "      v_message.level = 'warning';\n"
#                "      v_message.code = 'IMP002';\n"
#                "      v_message.title = 'Unable to delete expected ''{0}.{1}'' record.';\n"
#                "      v_message.affected_columns = NULL;\n"
#                "      v_message.affected_row_count = 0;\n"
#                "      v_message.content = 'Could not find a row with the primary key of';\n"
#                "      {3}\n"
#                "      RETURN NEXT v_message;\n"
#                "    END IF;\n").format(schemaName, storageTable.name,
#                                                   " AND ".join(map(lambda field: "{0} = p_{0}".format(field.column), primaryKeyColumns)),
#                                                    "\n      ".join(map(lambda field: "v_message.content = v_message.content || ' ' || p_{0};".format(field.column), primaryKeyColumns)))
#        
#        return Function(functionName, schemaName,
#                map(lambda field: field.columnDataType, primaryKeyColumns),
#                ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
#                "  {2}\n"
#                ") RETURNS SETOF shared.chimp_message AS $$\n"
#                "DECLARE\n"       
#                "  v_message shared.chimp_message;\n"
#                "  v_row_exists BOOLEAN;\n"
#                "BEGIN\n"  
#                "    {3}"
#                "END;\n"
#                "$$ LANGUAGE plpgsql;\n\n").format(schemaName, functionName, 
#                                                   ",\n  ".join(map(lambda field: "p_{0} {1}".format(field.column, field.columnDataType), primaryKeyColumns)),
#                                                   body))

    
    def getStorageInsertFunction(self, record, schemaName, storageTable, duplicatePrimaryKeyBehaviour):
        functionName = "{0}_insert".format(record.table)
        
        parameters = [SystemField("p_id", "bigint")]
        if schemaName == "import":
            parameters.append(SystemField("p_task_id", "integer"))
        if schemaName == "editable":
            parameters.append(SystemField("p_latest_source", "character varying"))            
        parameters += [SystemField("p_" + field.column, field.columnDataType) for field in record.getAllMappedFields()]        
        if schemaName == record.getDestinationTargetSchema():
            parameters += [SystemField("p_visibility", "integer"), SystemField("p_security", "integer")]

        if schemaName in record.alerts:
            alertInjection = record.alerts[schemaName].getDmlInjection("insert")
        else:
            alertInjection = ""

        if duplicatePrimaryKeyBehaviour == "ignore":
            duplicateClause = ("  WHEN unique_violation THEN\n"
                               "    NULL;\n")            
        elif duplicatePrimaryKeyBehaviour in ("notice", "warning", "error", "exception"):
            duplicateClause = ("  WHEN unique_violation THEN\n"
                               "    v_message = shared.make_{0}('IMP006','Could not insert, duplicate primary key',NULL,1,SQLERRM);\n"
                               "    RETURN NEXT v_message;\n".format(duplicatePrimaryKeyBehaviour))

        
        sql = ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
               "  {2}\n"
               ") RETURNS SETOF shared.chimp_message AS $$\n"
               "DECLARE\n"
               "  v_perform_insert boolean := TRUE;\n"
               "  v_message record;\n"               
               "BEGIN\n{3}"               
               "  IF v_perform_insert THEN\n"
               "    INSERT INTO {4}.{5} (\n"
               "     id{6}{7}{8}{9})\n" 
               "    VALUES (\n"
               "     p_id{10}{11}{12}{13});\n"
               "  END IF;\n"
               "EXCEPTION\n{14}"
               "  WHEN others THEN\n"
               "    v_message = shared.make_exception('IMP005','Unhandled exception while inserting',NULL,1,SQLERRM);\n"
               "    RETURN NEXT v_message;\n"
               "END;\n"
               "$$ LANGUAGE plpgsql;\n\n").format(                                                                                                      
                           schemaName, #0 
                           functionName, #1
                           ",\n  ".join(["{0} {1}".format(field.column, field.typeName) for field in parameters]), #2
                           alertInjection, #storageValidFunction.schema, #3 
                           storageTable.schema, #4
                           storageTable.name, #5                    
                           "" if schemaName != "editable" else ",\n     original_source,\n     latest_source", #6
                           "".join(map(lambda field: ",\n     {0}".format(field.column), record.getAllMappedFields())), #7
                           "" if schemaName != "import" else ",\n     last_affirmed_task_id,\n     created_task_id,\n     modified_task_id", #8
                           "" if schemaName != record.getDestinationTargetSchema() else ",\n     visibility,\n     security", #9
                           "" if schemaName != "editable" else ",\n     p_latest_source,\n     p_latest_source", #10
                           "".join(map(lambda field: ",\n     p_{0}".format(field.column), record.getAllMappedFields())), #11
                           "" if schemaName != "import" else ",\n     p_task_id,\n     p_task_id,\n     p_task_id", #12
                           "" if schemaName != record.getDestinationTargetSchema() else ",\n     p_visibility,\n     p_security", #13
                           duplicateClause) #14
               
                       
        return Function(functionName, schemaName, [field.typeName for field in parameters], sql)    
    
    def getStorageUpsertFunction(self, mode, record, schemaName, storageTable, whenNoDataFoundBehaviour):

        if mode=="update":
            actionVerb = "updating"
        elif mode=="merge":
            actionVerb = "merging"
            
        functionName = "{0}_{1}".format(record.table, mode)
        
        parameters = []
        if schemaName == "import":
            if mode=="merge":
                parameters.append(SystemField("p_id_to_use_if_inserting", "bigint"))
            parameters.append(SystemField("p_task_id", "integer"))
        else:
            parameters += [SystemField("p_id", "bigint"), SystemField("p_latest_source", "character varying")]
        parameters += [SystemField("p_" + field.column, field.columnDataType) for field in record.getAllMappedFields()]    
        if schemaName == record.getDestinationTargetSchema():
            parameters += [SystemField("p_visibility", "integer"), SystemField("p_security", "integer")]        


#        # How to deal with a row that's not there?
#        if whenNoDataFoundBehaviour in("notice","warning","error","exception"):
#            missingRowClause = ("    GET DIAGNOSTICS v_count = ROW_COUNT;\n"               
#                                "    IF v_count = 0 THEN\n"
#                                "      v_message.level = '{0}';\n"
#                                "      v_message.code = 'IMP001';\n"
#                                "      v_message.title = 'Unable to update expected ''{1}.{2}'' record.';\n"
#                                "      v_message.affected_columns = NULL;\n"
#                                "      v_message.affected_row_count = 0;\n"
#                                "      v_message.content = 'Could not find a row with the primary key of';\n"
#                                "      {3}\n"
#                                "      RETURN NEXT v_message;\n"
#                                "    END IF;\n".format(whenNoDataFoundBehaviour, #0
#                                                       storageTable.schema, #1
#                                                       storageTable.name, #2
#                                                       "\n      ".join(map("v_message.content = v_message.content || ' ' || p_{0};".format, record.primaryKeyColumns)))) #3

                        
        sql =  ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
                "  {2}\n"
                ") RETURNS SETOF shared.chimp_message AS $$\n"
                "DECLARE\n"
                "  v_count INTEGER;\n"
                "  v_perform_{3} boolean;\n"
                "  v_message shared.chimp_message;\n").format(schemaName, #0
                                                              functionName, #1
                                                              ",\n  ".join(["{0} {1}".format(field.column, field.typeName) for field in parameters]), #2
                                                              mode) #3

        if schemaName in record.alerts or mode=="merge":
            sql += "  v_old_record record;\n"

                
        if schemaName in record.alerts:
            alertInjection = record.alerts[schemaName].getDmlInjection(mode)
            #sql += "  v_old_record {0}.all_{1}_{2}_params;\n".format(ALERTS_SCHEMA, schemaName, record.table)
            if storageTable.schema =='editable':
                identifierInfo = "id '||p_id::character varying"
            else:
                identifierInfo = "key ('||{0}||')'".format("||', '||".join(map(lambda x:"p_{0}::character varying".format(x), record.primaryKeyColumns)))

            additionalExceptions = ("  WHEN NO_DATA_FOUND THEN\n"
                                    "    v_message = ('{0}','NODATA','Unable to {3} expected ''{1}.{2}'' record.',NULL,0,'Could not find {4});\n"
                                    "    RETURN NEXT v_message;\n".format(whenNoDataFoundBehaviour, #0
                                                                          storageTable.schema, #1
                                                                          storageTable.name, #2
                                                                          mode, #3
                                                                          identifierInfo)) #4 

        else:
            alertInjection = ""
            additionalExceptions = ""
            
         

        if mode=="merge":
            if schemaName in record.alerts:
                insertAlertInjection = record.alerts[schemaName].getDmlInjection("insert",flagVariableOverride="v_perform_merge")
            else:
                insertAlertInjection = "  v_perform_merge = TRUE;\n"

            mergeWrapUp = ("\nELSE\n"
                           "  -- No record, so do an insert instead\n{0}"
                           "  IF v_perform_merge THEN\n"
                           "    INSERT INTO {1}.{2} (\n"
                           "     id{3}{4}{5}{6})\n" 
                           "    VALUES (\n"
                           "     {11}{7}{8}{9}{10});\n"                           
                           "  END IF;\n"
                           "END IF;\n").format(insertAlertInjection, #0
                                               storageTable.schema, #1
                                               storageTable.name, #2                    
                                               "" if schemaName != "editable" else ",\n     original_source,\n     latest_source", #3
                                               "".join(map(lambda field: ",\n     {0}".format(field.column), record.getAllMappedFields())), #4
                                               "" if schemaName != "import" else ",\n     last_affirmed_task_id,\n     created_task_id,\n     modified_task_id", #5
                                               "" if schemaName != record.getDestinationTargetSchema() else ",\n     visibility,\n     security", #6
                                               "" if schemaName != "editable" else ",\n     p_latest_source,\n     p_latest_source", #7
                                               "".join(map(lambda field: ",\n     p_{0}".format(field.column), record.getAllMappedFields())), #8
                                               "" if schemaName != "import" else ",\n     p_task_id,\n     p_task_id,\n     p_task_id", #9
                                               "" if schemaName != record.getDestinationTargetSchema() else ",\n     p_visibility,\n     p_security", #10
                                               "p_id_to_use_if_inserting" if schemaName =="import" else "p_id") #11
        else:
            mergeWrapUp=""

        
        sql += "BEGIN\n"
        
        
        if mode=="merge" and schemaName not in record.alerts:
            sql += ("\n  SELECT {0}\n"
                    "  INTO v_old_record\n"
                    "  FROM {1}.{2}\n"
                    "  WHERE {3};\n"
                    "\nIF FOUND THEN\n").format(",".join(map(lambda x:x.column, record.getAllMappedFields())), #0
                                                                   schemaName,#1
                                                                   record.table,#2
                                                                   "id = p_id" if schemaName == "editable" else " AND ".join(map(lambda x:"{0} = p_{0}".format(x), record.primaryKeyColumns)))

        
                
        sql += ("  v_perform_{8} = TRUE;{0}\n"
                "  IF v_perform_{8} THEN\n"
                "    UPDATE {1}.{2} SET\n"
                "      {3},\n"
                "      {4}{5}\n"
                "    WHERE {6};\n"
                "  END IF;\n{10}\n"
                "EXCEPTION\n{7}"
                "  WHEN others THEN\n"
                "    v_message = shared.make_exception('IMP006','Unhandled exception while {9}',NULL,1,SQLERRM);\n"
                "    RETURN NEXT v_message;\n"
                "END;\n"
                "$$ LANGUAGE plpgsql;\n\n".format(alertInjection, #0                                                                            
                                                  storageTable.schema, #1
                                                  storageTable.name, #2                     
                                                  "last_affirmed = now(),\n      last_affirmed_task_id = p_task_id" if schemaName == "import" else "latest_source = p_latest_source", #3
                                                  ",\n      ".join(map(lambda field: "{0} = p_{0}".format(field.column), record.getAllMappedFields())), #4
                                                  "" if schemaName != record.getDestinationTargetSchema() else ",\n      visibility = p_visibility,\n      security = p_security", #5
                                                  "id = p_id" if (schemaName == "editable" or not record.hasPrimaryKey()) else " AND ".join(map("{0} = p_{0}".format, record.primaryKeyColumns)), #6
                                                  additionalExceptions, #7
                                                  mode, #8
                                                  actionVerb, #9
                                                  mergeWrapUp)) #10
                 
        return Function(functionName, schemaName, [field.typeName for field in parameters],sql)          

    
    def getStorageDeleteFunction(self, record, schemaName, storageTable, whenNoDataFoundBehaviour):
        functionName = "{0}_delete".format(record.table)   
        
        parameters = []
        if schemaName == "editable":
            parameters.append(SystemField("p_id", "bigint"))
        else:
            parameters.append(SystemField("p_task_id", "integer"))
            parameters += [SystemField("p_" + field.column, field.columnDataType) for field in record.getPrimaryKeyFields()]



        sql = ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
               "  {2}\n"
               ") RETURNS SETOF shared.chimp_message AS $$\n"                
               "DECLARE\n"
               "  v_perform_delete boolean;\n"
               "  v_message record;\n"
               "  v_result shared.chimp_message;\n".format(schemaName,
                                                           functionName,
                                                           ",\n  ".join(["{0} {1}".format(field.column, field.typeName) for field in parameters])))                                                                    

        
        if schemaName in record.alerts:
            alertInjection = record.alerts[schemaName].getDmlInjection("delete")
            sql += "  v_old_record record;\n"
            
            if storageTable.schema =='editable':
                identifierInfo = "id '||p_id::character varying"
            else:
                identifierInfo = "key ('||{0}||')'".format("||', '||".join(map(lambda x:"p_{0}::character varying".format(x), record.primaryKeyColumns)))
            
            additionalExceptions = ("  WHEN NO_DATA_FOUND THEN\n"
                                    "    v_result = ('{0}','NODATA','Unable to delete expected ''{1}.{2}'' record.',NULL,0,'Could not find {3});\n"
                                    "    RETURN NEXT v_result;\n".format(whenNoDataFoundBehaviour, #0
                                                                          storageTable.schema, #1
                                                                          storageTable.name, #2
                                                                          identifierInfo)) #3 
                                    
# ("level" character varying(30),
#    code character varying(30),
#    title character varying(200),
#    affected_columns character varying(2000),
#    affected_row_count integer,
#    "content" character varying(4000));                                    
#                                    
#                                    
#                                    "    v_message.level = '{0}';\n"
#                                    "    v_message.code = 'NODATA';\n"
#                                    "    v_message.title = 'Unable to delete expected ''{1}.{2}'' record.';\n"
#                                    "    v_message.affected_columns = NULL;\n"
#                                    "    v_message.affected_row_count = 0;\n"
#                                    "    v_message.content = 'Could not find a row with id '||p_id::character varying;\n"
#                                    "    RETURN NEXT v_message;\n".format(whenNoDataFoundBehaviour, #0
#                                                                          storageTable.schema, #1
#                                                                           storageTable.name)) #3

        else:
            alertInjection = ""
            additionalExceptions = ""

        
        sql+= ("BEGIN\n"
               "  v_perform_delete = TRUE;\n{0}"
               "  IF v_perform_delete THEN\n"                               
               "    DELETE FROM {1}.{2}\n"
               "    WHERE {3};\n"
               "  END IF;\n"  
               "EXCEPTION\n{4}"
               "  WHEN others THEN\n"
               "    v_message = shared.make_exception('IMP007','Unhandled exception while deleting',NULL,1,SQLERRM);\n"
               "    RETURN NEXT v_message;\n"                
               "END;\n"
               "$$ LANGUAGE plpgsql;\n\n".format(alertInjection, #0
                                                 storageTable.schema, #1
                                                 storageTable.name, #2
                                                 "id = p_id" if (schemaName == "editable" or not record.hasPrimaryKey()) else " AND ".join(map("{0} = p_{0}".format, record.primaryKeyColumns)),
                                                 additionalExceptions)) #3       
                               
        return Function(functionName, schemaName, 
                [field.typeName for field in parameters],sql)       

#    
#    def getStorageMergeFunction(self, record, schemaName, storageTable, storageInsertFunction, storageUpdateFunction):
#        functionName = "{0}_merge".format(record.table)
#        
#        parameters = [SystemField("p_id", "bigint")]
#        if schemaName == "import":
#            parameters.append(SystemField("p_task_id", "integer"))
#        else:
#            parameters.append(SystemField("p_source", "character varying"))            
#        parameters += [SystemField("p_" + field.column, field.columnDataType) for field in record.getAllMappedFields()]        
#        if schemaName == record.getDestinationTargetSchema():
#            parameters += [SystemField("p_visibility", "integer"), SystemField("p_security", "integer")]
#        
#        return Function(functionName, schemaName, 
#                [field.typeName for field in parameters],
#                ("CREATE OR REPLACE FUNCTION {0}.{1} (\n"
#                "  {2}\n"               
#                ") RETURNS SETOF shared.chimp_message AS $$\n"
#                "DECLARE\n"
#                "  v_perform_insert boolean;\n"
#                "  v_update_occurred boolean;\n"
#                "  v_message record;\n"
#                "BEGIN\n"
#                "  v_perform_insert = TRUE;\n"
#                "  v_update_occurred = TRUE;\n"
#                "  FOR v_message IN SELECT * FROM {3}.{4}(\n"
#                "    {5},\n"
#                "    {6}{7}\n"
#                ") LOOP\n"
#                "    IF v_message.level in('error','exception') THEN\n"
#                "      v_perform_insert = FALSE;\n"
#                "    END IF;\n"
#                "    IF v_message.code='IMP001' THEN\n"
#                "      v_update_occurred = FALSE;\n"
#                "    ELSE\n"
#                "      RETURN NEXT v_message;\n"
#                "    END IF;\n"                                
#                "  END LOOP;\n"
#                "  IF v_perform_insert AND (NOT v_update_occurred) THEN\n"
#                "    FOR v_message IN SELECT * FROM {8}.{9}(\n"
#                "      p_id,\n"
#                "      {10},\n"
#                "      {11}{12}\n"
#                "  ) LOOP\n"
#                "      RETURN NEXT v_message;\n"
#                "    END LOOP;\n"
#                "  END IF;\n"
#                "EXCEPTION\n"
#                "  WHEN others THEN\n"
#                "    v_message = shared.make_exception('IMP008','Unhandled exception while merging',NULL,1,SQLERRM);\n"
#                "    RETURN NEXT v_message;\n"
#                "END;\n"
#                "$$ LANGUAGE plpgsql;\n\n").format(schemaName, functionName,
##                        "p_task_id integer" if schemaName == "import" else "p_source character varying",
##                        ",\n  ".join(map(lambda field: "p_{0} {1}".format(field.column, field.columnDataType), record.getAllMappedFields())),
##                        "" if schemaName == record.getDestinationTargetSchema() else ",\n  p_visibility integer,\n  p_security integer",
#                        ",\n  ".join(["{0} {1}".format(field.column, field.typeName) for field in parameters]),
#                        
#                        storageUpdateFunction.schema, storageUpdateFunction.name,
#                        "p_task_id" if schemaName == "import" else "p_id,\n    p_source",
#                        ",\n    ".join(["p_" + field.column for field in record.getAllMappedFields()]),
#                        "" if schemaName != record.getDestinationTargetSchema() else ",\n    p_visibility,\n    p_security",
#                        
#                        storageInsertFunction.schema, storageInsertFunction.name,
#                        "p_task_id" if schemaName == "import" else "p_source",
#                        ",\n      ".join(["p_" + field.column for field in record.getAllMappedFields()]),
#                        "" if schemaName != record.getDestinationTargetSchema() else ",\n      p_visibility,\n      p_security"))
    
    def getSharedToMergeIntoEditableView(self, thisRecord):
        viewName = "{0}_to_merge_into_editable".format(thisRecord.table)
        targetTablesName = thisRecord.table        
        return View(viewName, SHARED_SCHEMA, 
                ("CREATE OR REPLACE VIEW {0}.{1} AS \n"
                 "  SELECT\n"
                 "    i.id,\n"
                 "    e.id IS NOT NULL AS editable_record_exists,\n"
                 "    {2},\n"
                 "    i.created AS created,\n"
                 "    i.modified AS modified,\n"
                 "    {3},\n"
                 "    e.visibility AS e_visibility,\n"
                 "    e.security AS e_security\n"
                 "  FROM\n"
                 "    {4}.{5} AS i LEFT OUTER JOIN {6}.{5} AS e ON (\n"
                 "      {7}"
                 "    );\n\n").format(SHARED_SCHEMA, viewName,
                            ",\n    ".join(map(lambda field: "i.{0} AS {0}".format(field.column), thisRecord.getAllMappedFields())),
                            ",\n    ".join(map(lambda field: "e.{0} AS e_{0}".format(field.column), thisRecord.getAllMappedFields())),
                            IMPORT_SCHEMA, targetTablesName,
                            EDITABLE_SCHEMA,
                            "\n      AND ".join(map("i.{0} = e.{0}".format, thisRecord.primaryKeyColumns))))

    
    def getStorageSpecificationExistsFunction(self, schemaName):
        functionName = "{0}_exists".format(self.specification.name)
        
        tables = [record.table for record in self.specification.records 
                  if record.useful and (schemaName == "import" or record.editable)]
        
        return Function(
            functionName, schemaName, [], 
            "CREATE OR REPLACE FUNCTION {0}.{1}() RETURNS boolean AS $$\n"
            "  DECLARE\n"
            "  {2}\n"   
            "  BEGIN\n"
            "      {3}\n"
            "      RETURN ({4});\n"
            "  END;\n"
            "$$ LANGUAGE plpgsql;\n\n".format(schemaName, functionName,
                   "\n  ".join(["v_{0}_populated boolean;".format(record.table) for record in self.specification.records if record.useful]),
                   "\n      ".join(["SELECT exists(select 1 from {0}.{1} limit 1) INTO v_{1}_populated;".format(schemaName, table) for table in tables]),
                   " OR ".join(["v_{0}_populated".format(table) for table in tables])))

    
    def getStageDeleteDuplicatesDML(self, record, stageTable):

#        whereClause=" AND ".join(map(lambda x:"source.{0}=duplicates.{0}".format(x), self.specification.stageDuplicateColumns))
#        return DML(("DELETE FROM {0}.{1} WHERE id IN (SELECT "
#                    "source.id from {0}.{1} AS source, (SELECT {2}, max(id) AS max_id "
#                    "FROM {0}.{1} AS dup GROUP BY {2} HAVING COUNT(*)>1) AS duplicates "
#                    "WHERE {4} and max_id != source.id);\n").format(stageTable.schema, stageTable.name,
#                                           ", ".join(self.specification.stageDuplicateColumns), record.table, whereClause))

        if self.specification.stageDuplicateColumns is not None:
            whereClause=" AND ".join(map(lambda x:"source.{0}=duplicates.{0}".format(x), self.specification.stageDuplicateColumns))
            dml = ("DELETE FROM {0}.{1} WHERE id IN (SELECT "
                    "source.id from {0}.{1} AS source, (SELECT {2}, max(id) AS max_id "
                    "FROM {0}.{1} AS dup GROUP BY {2} HAVING COUNT(*)>1) AS duplicates "
                    "WHERE {4} and max_id != source.id);\n").format(stageTable.schema, stageTable.name, ", ".join(self.specification.stageDuplicateColumns), record.table, whereClause)
        else:
            dml = "\n"
        return DML((dml))
   
#                duplicateFile.write("SELECT count(*) FROM stage.%s_sequencer WHERE table_name='%s' and record_id NOT IN (SELECT MAX(id) FROM %s.%s AS dup GROUP BY %s);\n" %(specification.name, record.table, nativeStageSchema, nativeTable, specification.stageDuplicateColumns))
    
    
    def getStageGeometryAddDML(self, field, srid, stageTable):
        return DML("SELECT AddGeometryColumn('{0}', '{1}', '{2}', {3}, 'GEOMETRY', 2);\n\n".format(
            stageTable.schema, stageTable.name, field.column, srid))




               
#.format(STAGE_SCHEMA, functionName, delegate.schema, delegate.name, record.tabl
class SystemField:
    def __init__(self, column, typeName, size = None, nullable = False, default = None):
        self.column = column
        self.typeName = typeName
        
        #TODO: sort these different names for the same thing
        self.columnTypeName = self.typeName
        self.columnDataType = self.typeName
        
        self.size = size
        self.nullable = nullable
        self.default = default

    def getSQLFragment(self):        
        f = self.column + " " + self.typeName
        if (self.size is not None):
            f += "(" + str(self.size) + ")"
        if (not self.nullable):
            f += " NOT NULL"
        if (self.default is not None):
            f += " DEFAULT " + self.default
        return f


EDITABLE_SYSTEM_FIELDS = [SystemField("original_source", "character varying", size=30), 
                          SystemField("latest_source", "character varying", size=30)]

IMPORT_SYSTEM_FIELDS = [SystemField("last_affirmed", "timestamp with time zone", default="now()"),
                        SystemField("last_affirmed_task_id", "integer"),
                        SystemField("created_task_id", "integer"),
                        SystemField("modified_task_id", "integer")]             

                        
class DBObject:        
    def __init__(self, name, schema, ddl=None, droppable=True):
        self.name = name
        self.schema = schema
        self.cascadeOnDrop = False
        self.ddl = ddl  
        self.droppable = droppable      
        
    def getDropStatement(self):
        if self.schema is not None:
            dml = "{0}DROP {1} IF EXISTS {2}.{3}{4};\n".format("" if self.droppable else "-- ", self.typeName, self.schema, self.name, " CASCADE" if self.cascadeOnDrop else "")
        else:
            dml = "{0}DROP {1} IF EXISTS {2}{3};\n".format("" if self.droppable else "-- ", self.typeName, self.name, " CASCADE" if self.cascadeOnDrop else "")            
        return(dml)
    
class Index(DBObject):
    typeName = "INDEX"
    def __init__(self, name, tableName, schema, ddl=None, droppable=True):
        DBObject.__init__(self, name, schema, ddl, droppable)
        self.tableName = tableName
                
class Table(DBObject):
    typeName = "TABLE"
    def __init__(self, name, schema, ddl=None, droppable=True):
        DBObject.__init__(self, name, schema, ddl, droppable)
        self.cascadeOnDrop = True
        
    def getTruncateStatement(self):
        return "TRUNCATE TABLE {0}.{1};\n".format(self.schema, self.name)
    
class Function(DBObject):
    typeName = "FUNCTION"
    def __init__(self, name, schema, paramTypes, ddl=None, droppable=True):
        DBObject.__init__(self, name, schema, ddl, droppable)
        self.paramTypes = paramTypes
        
    def getDropStatement(self):
        return "{0}DROP {1} IF EXISTS {2}.{3}({4}) CASCADE;\n".format("" if self.droppable else "-- ", self.typeName, self.schema, self.name, ", ".join(self.paramTypes))
    
class Sequence(DBObject):
    typeName = "SEQUENCE"
    def __init__(self, name, schema, ddl=None, droppable=True):
        DBObject.__init__(self, name, schema, ddl, droppable)
    
class Type(DBObject):
    typeName = "TYPE"
    def __init__(self, name, schema, ddl=None, droppable=True):
        DBObject.__init__(self, name, schema, ddl, droppable)
        self.cascadeOnDrop = True

class Trigger(DBObject):
    typeName = "TRIGGER"
    def __init__(self, name, tableName, functionName, schema, ddl=None, droppable=True):
        DBObject.__init__(self, name, schema, ddl, droppable)
        self.tableName = tableName
        self.functionName = functionName

class Schema(DBObject):
    typeName = "SCHEMA"
    def __init__(self, name, ddl=None, droppable=True):
        DBObject.__init__(self, name, None, ddl, droppable)
        
class DML(DBObject):
    typeName = "DML"
    def __init__(self, ddl, dropDdl=None, droppable=True):
        DBObject.__init__(self, None, None, ddl, droppable)
        self.dropDdl = dropDdl
        
    def getDropStatement(self):
        return self.dropDdl

class Rule(DBObject):
    typeName = "RULE"
    def __init__(self, name, ddl=None, droppable=True):
        DBObject.__init__(self, name, None, ddl, droppable)
    
class View(DBObject):
    typeName = "VIEW"
    def __init__(self, name, schema, ddl=None, droppable=True):
        DBObject.__init__(self, name, schema, ddl, droppable)

class DBObjectRegistry:
    
    indexes = []
    tables = []
    functions = []
    sequences = []
    types = []
    views = []
    triggers = []
    rules = []
    dml = []
    schemas = []

    def __init__(self):
        None
        
    # EWWWW. TODO: Put in a dict or something.
    def register(self, dbObj):
        if (dbObj.typeName == "TABLE"):
            self.tables.append(dbObj)
        elif (dbObj.typeName == "SEQUENCE"):
            self.sequences.append(dbObj)
        elif (dbObj.typeName == "VIEW"):
            self.views.append(dbObj)
        elif (dbObj.typeName == "INDEX"):
            self.indexes.append(dbObj)
        elif (dbObj.typeName == "TYPE"):
            self.types.append(dbObj)
        elif (dbObj.typeName == "FUNCTION"):
            self.functions.append(dbObj)
        elif (dbObj.typeName == "TRIGGER"):
            self.triggers.append(dbObj)
        elif (dbObj.typeName == "RULE"):
            self.rules.append(dbObj)
        elif (dbObj.typeName == "SCHEMA"):
            self.schemas.append(dbObj)
        elif (dbObj.typeName == "DML"):
            self.dml.append(dbObj)
        else:
            raise RuntimeError(dbObj.typeName)

    #  ALL DEPRECATED - DO NOT USE!        
    def registerIndex(self, table, indexName, ddl, schema):
        self.indexes.append(Index(indexName, table, schema, ddl.strip("\n")))
        
    def registerTable(self, name, schema):
        self.tables.append(Table(name, schema))
        
    def registerFunction(self, name, schema, paramTypes):
        self.functions.append(Function(name, schema, paramTypes))
        
    def registerSequence(self, name, schema):
        self.sequences.append(Sequence(name, schema))
        
    def registerType(self, name, schema):
        self.types.append(Type(name, schema))
        
    def registerView(self, name, schema):
        self.views.append(View(name, schema))
    #  ***END*** ALL DEPRECATED - DO NOT USE!    
        
    def writeDropScript(self, path):        
        with open(path, "w") as f:
            for dbObj in list(itertools.chain(self.dml, self.functions, self.tables, 
                                              self.sequences, self.types, self.views, self.schemas)):
                sql = dbObj.getDropStatement()
                if sql is not None:
                    f.write(sql);

