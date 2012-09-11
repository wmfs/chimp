ALERTS_SCHEMA = "alerts"
from chimpsql import DBObjectRegistry
import chimpsql
import chimpspec

class Alert:

#"  DELETE FROM {0}.{1}_{2}_alerts\n"
#               "  WHERE {5};\n"
#"\n  AND ".join(map(lambda x:"{0}=p_{0}".format(x.column), self.primaryKeyFields))

    

    def getOldRecordSelectList(self):
        return ",".join(map(lambda x:x.column, self.fields))

    def getDmlInjection(self, dmlEvent, flagVariableOverride=None):

        dml = ""
        
        if flagVariableOverride is None:
            flagVariable = "v_perform_{0}".format(dmlEvent)
        else:
            flagVariable = flagVariableOverride
        
        if dmlEvent=="insert":                        
            paramClause = ",".join(map(lambda x:"p_{0}".format(x.column), self.fields))
            paramClause = "("+paramClause+")"
            alertInsertValues = ",".join(map(lambda x:"p_{0}".format(x.column), self.primaryKeyFields))
            revisedDmlEvent = "insert"              

        elif dmlEvent=="update":                        
            paramClause1 = ",".join(map(lambda x:"v_old_record.{0}".format(x.column), self.fields))
            paramClause2 = ",".join(map(lambda x:"p_{0}".format(x.column), self.fields))
            paramClause = "({0}),\n     ({1})".format(paramClause1,paramClause2)
            alertInsertValues = ",".join(map(lambda x:"p_{0}".format(x.column), self.primaryKeyFields))
            strictClause = "STRICT "
            revisedDmlEvent = "update"

        elif dmlEvent=="merge":
            paramClause1 = ",".join(map(lambda x:"v_old_record.{0}".format(x.column), self.fields))
            paramClause2 = ",".join(map(lambda x:"p_{0}".format(x.column), self.fields))
            paramClause = "({0}),\n     ({1})".format(paramClause1,paramClause2)
            alertInsertValues = ",".join(map(lambda x:"p_{0}".format(x.column), self.primaryKeyFields))
            strictClause = ""
            revisedDmlEvent = "update"

        elif dmlEvent=="delete":                        
            paramClause = ",".join(map(lambda x:"v_old_record.{0}".format(x.column), self.fields))
            paramClause = "("+paramClause+")"              
            alertInsertValues = ",".join(map(lambda x:"v_old_record.{0}".format(x.column), self.primaryKeyFields))
            strictClause = "STRICT "
            revisedDmlEvent = "delete"

        if dmlEvent in("update", "merge", "delete"):
            dml += ("\n\n  SELECT {0}\n"
                    "  INTO {4}v_old_record\n"
                    "  FROM {1}.{2}\n"
                    "  WHERE {3};\n").format(self.getOldRecordSelectList(), #0
                                             self.sourceSchema,#1
                                             self.sourceName,#2
                                             "id = p_id" if self.sourceSchema == "editable" else " AND ".join(map(lambda x:"{0} = p_{0}".format(x.column), self.primaryKeyFields)), #3
                                             strictClause)#4
 
            if dmlEvent == "merge":
                dml += "\nIF {0} THEN\n".format(" AND ".join(map(lambda x:"v_old_record.{0} IS NOT NULL".format(x.column), self.primaryKeyFields)))
                
            dml += ("\n  DELETE FROM {0}.{1}_{2}_alerts\n"
                    "  WHERE {3};\n\n").format(ALERTS_SCHEMA, #0
                                               self.sourceSchema,#1
                                               self.sourceName,#2
                                               "id = p_id" if self.sourceSchema == "editable" else " AND ".join(map(lambda x:"{0} = p_{0}".format(x.column), self.primaryKeyFields))) #3)


#             dml += ("\n  DELETE FROM {0}.{1}_{2}_alerts\n"
#                    "  WHERE {3};\n\n"
#                    "  SELECT {4}\n"
#                    "  INTO STRICT v_old_record\n"
#                    "  FROM {1}.{2}\n"
#                    "  WHERE {3};\n\n").format(ALERTS_SCHEMA, #0
#                                             self.sourceSchema,#1
#                                             self.sourceName,#2
#                                             "id = p_id" if self.sourceSchema == "editable" else " AND ".join(map(lambda x:"{0} = p_{0}".format(x.column), record.primaryKeyFields)),#3
#                                             self.getOldRecordSelectList()) #4
# 
        
        dml += ("  FOR v_message IN SELECT * FROM {0}.get_{1}_{2}_{7}_alerts(\n"
               "    {4}) LOOP\n"
               "    IF v_message.level in('error','exception') THEN\n"
               "      {3} = FALSE;\n"
               "    END IF;\n"
               "    INSERT INTO {0}.{1}_{2}_alerts (record_id,{5},domain,level,code,title,affected_columns,affected_row_count,content)\n"
               "    VALUES (p_id,{6},'alert',v_message.level,v_message.code,v_message.title,v_message.affected_columns,v_message.affected_row_count,v_message.content);\n"
               "    RETURN NEXT v_message;\n"
               "  END LOOP;\n").format(ALERTS_SCHEMA, #0
                                       self.sourceSchema,#1
                                       self.sourceName,#2
                                       flagVariable, #3
                                       paramClause, #4
                                       ",".join(map(lambda x:x.column, self.primaryKeyFields)), #5
                                       alertInsertValues, #6
                                       revisedDmlEvent) #7
        return dml

    def getSpecSequence(self):
        sequenceName = "{0}_seq".format(self.specificationName)         #
        return chimpsql.Sequence(sequenceName, ALERTS_SCHEMA, 
                        "CREATE SEQUENCE {0}.{1} START WITH 1;\n\n".format(ALERTS_SCHEMA, sequenceName))


    def getRegistryPkIndex(self, table):
        indexName = "{0}_pk_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} ({3});\n\n".format(indexName, ALERTS_SCHEMA, table.name, ",".join(map(lambda pk:pk.column, self.primaryKeyFields))))

    def getRegistryEnabledIndex(self, table):
        indexName = "{0}_enabled_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (alert_enabled);\n\n".format(indexName, ALERTS_SCHEMA, table.name ))

    def getRegistryOnInsertIndex(self, table):
        indexName = "{0}_insert_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (on_insert);\n\n".format(indexName, ALERTS_SCHEMA, table.name ))

    def getRegistryOnUpdateIndex(self, table):
        indexName = "{0}_update_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (on_update);\n\n".format(indexName, ALERTS_SCHEMA, table.name))

    def getRegistryOnDeleteIndex(self, table):
        indexName = "{0}_delete_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (on_delete);\n\n".format(indexName, ALERTS_SCHEMA, table.name))

    def getRegistryStageIndex(self, table):
        indexName = "{0}_stage_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (from_stage);\n\n".format(indexName, ALERTS_SCHEMA, table.name))

    def getRegistryImportIndex(self, table):
        indexName = "{0}_import_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (from_import);\n\n".format(indexName, ALERTS_SCHEMA, table.name))

    def getRegistryEditableIndex(self, table):
        indexName = "{0}_editable_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (from_editable);\n\n".format(indexName, ALERTS_SCHEMA, table.name))

    def getRegistryMvIndex(self, table):
        indexName = "{0}_mv_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (from_mv);\n\n".format(indexName, ALERTS_SCHEMA, table.name))

    
    
    def getParamType(self):
        typeName = "all_{0}_{1}_params".format(self.sourceSchema, self.sourceName)
        ddl = ("CREATE TYPE {0}.{1} AS     (\n  "
               "{2}").format(ALERTS_SCHEMA, typeName, ",\n  ".join(map(lambda x:x.strippedColumnClause(None, False), self.fields)))            
        ddl += ""
        ddl += ");\n\n"
        return chimpsql.Type(typeName, ALERTS_SCHEMA, ddl)

    def getAlertRecordIdIndex(self, table):
        indexName = "alert_{0}_record_id_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} (record_id);\n\n".format(indexName, ALERTS_SCHEMA, table.name))

    def getAlertPkIndex(self, table):
        indexName = "alert_{0}_pk_idx".format(table.name)                
        return chimpsql.Index(indexName, table.name, ALERTS_SCHEMA, 
                     "CREATE INDEX {0} ON {1}.{2} ({3});\n\n".format(indexName, ALERTS_SCHEMA, table.name, ",".join(map(lambda pk:pk.column, self.primaryKeyFields))))

    def getAlertTable(self, sequence):
        tableName = "{0}_{1}_alerts".format(self.sourceSchema, self.sourceName)        
        primaryKeyColumns =""
        for field in self.primaryKeyFields:
            primaryKeyColumns += "\n  {0},".format(field.strippedColumnClause(None, False))        
        ddl = ("CREATE TABLE {0}.{1} (\n"
               "  id bigint PRIMARY KEY default nextval('{3}.{4}'),\n"
               "  record_id bigint NOT NULL,\n{2}\n"
               "  domain character varying(30) NOT NULL,\n"               
               "  level character varying(30) NOT NULL,\n"
               "  code character varying(30) NOT NULL,\n"
               "  title character varying(200) NOT NULL,\n"
               "  affected_columns character varying(2000),\n"
               "  affected_row_count integer,\n"
               "  content character varying(4000),\n"
               "  created timestamp with time zone NOT NULL DEFAULT now());\n").format(ALERTS_SCHEMA, tableName, primaryKeyColumns, sequence.schema, sequence.name)
        return chimpsql.Table(tableName, ALERTS_SCHEMA, ddl)

    
    def getRegistryTable(self, sequence):
        tableName = "{0}_registry".format(self.sourceName)        
        primaryKeyColumns =""
        for field in self.primaryKeyFields:
            primaryKeyColumns += "\n  {0},".format(field.strippedColumnClause(None, False))        
        ddl = ("CREATE TABLE {0}.{1} (\n"
               "  id bigint PRIMARY KEY default nextval('{3}.{4}'),\n"
               "  alert_enabled boolean NOT NULL,\n"
               "  domain character varying(30) NOT NULL,\n"
               "  message_level character varying(30) NOT NULL,\n"
               "  alert_name character varying(200) NOT NULL,\n"
               "  alert_title character varying(200) NOT NULL,\n"
               "  associate_with_record boolean NOT NULL,{2}\n"
               "  from_stage boolean NOT NULL,\n"               
               "  from_import boolean NOT NULL,\n"
               "  from_editable boolean NOT NULL,\n"
               "  from_mv boolean NOT NULL,\n"
               "  on_insert boolean NOT NULL,\n"
               "  on_update boolean NOT NULL,\n"
               "  on_delete boolean NOT NULL,\n"
               "  message character varying(2000) NOT NULL,\n"
               "  message_inputs character varying(2000),\n"
               "  sql_expression character varying(5000),\n"
               "  affected_columns character varying(2000));\n\n").format(ALERTS_SCHEMA, tableName, primaryKeyColumns, sequence.schema, sequence.name)
        return chimpsql.Table(tableName, ALERTS_SCHEMA, ddl)





    def getGeneratorFunction(self, schema, table, type, dmlEvent):
        functionName = "get_{0}_{1}_{2}_alerts".format(schema, self.sourceName, dmlEvent)
        paramWhere=""
        i = 1
        for field in self.primaryKeyFields:
            paramWhere += "    AND a.{0}=${1}\n".format(field.column, i)
            i += 1
        
        if dmlEvent == "insert":
            prefix = "new"
            firingEvent="on_insert"
            paramText = "p_new {0}.{1}".format(ALERTS_SCHEMA, type.name)
            returnTypes=["{0}.{1}".format(ALERTS_SCHEMA, type.name)]
            contentRecordPrefixes="p_new"
        
        elif dmlEvent == "update":
            prefix = "new"
            firingEvent="on_update"
            paramText = "p_old {0}.{1}, p_new {0}.{1}".format(ALERTS_SCHEMA, type.name)
            returnTypes=["{0}.{1}".format(ALERTS_SCHEMA, type.name),"{0}.{1}".format(ALERTS_SCHEMA, type.name)]
            contentRecordPrefixes="p_old,p_new"

        elif dmlEvent == "delete":
            prefix = "old"
            firingEvent="on_delete"
            paramText = "p_old {0}.{1}".format(ALERTS_SCHEMA, type.name)
            returnTypes=["{0}.{1}".format(ALERTS_SCHEMA, type.name)]
            contentRecordPrefixes="p_old"

        sql =("CREATE OR REPLACE FUNCTION {0}.{1}({7})\n"
              "  RETURNS SETOF shared.chimp_message AS\n"
              "$BODY$\n"
              "DECLARE\n"
              "  v_alert_definition record;\n"                         
              "  v_should_fire boolean;\n"
              "  v_content character varying;\n"
              "  v_alert shared.chimp_message;\n"
              "BEGIN\n"
              "  FOR v_alert_definition IN EXECUTE\n" 
              "   'SELECT alert_title,alert_name,message_level,message,message_inputs,sql_expression,affected_columns\n"
              "    FROM {0}.{2}\n"
              "    WHERE NOT associate_with_record\n"
              "    AND {4}\n"
              "    AND from_{8}\n"
              "    AND alert_enabled\n"
              "    UNION\n"
              "    SELECT alert_title,alert_name,message_level,message,message_inputs,sql_expression,affected_columns\n"
              "    FROM {0}.{2} AS a\n"
              "    WHERE associate_with_record\n{3}"
              "    AND {4}\n"
              "    AND from_{8}\n"
              "    AND alert_enabled'\n"
              "    USING {5} LOOP\n\n"
              "    -- Should this alert fire?\n").format(ALERTS_SCHEMA, #0 
                                                          functionName, #1
                                                          table.name, #2
                                                          paramWhere, #3
                                                          firingEvent,#4
                                                          ", ".join(map(lambda x:"p_{0}.{1}".format(prefix, x.column), self.primaryKeyFields)), #5
                                                          type.name, #6
                                                          paramText, #7
                                                          schema) #8

        if dmlEvent=="insert":             
            sql += ("    EXECUTE 'select '||REPLACE(v_alert_definition.sql_expression, 'new.', '$1.')\n"
                    "    INTO v_should_fire\n"
                    "    USING p_new;\n\n")

        elif dmlEvent=="update":             
            sql += ("    EXECUTE 'select '||REPLACE(REPLACE(v_alert_definition.sql_expression, 'old.', '$1.'), 'new.', '$2.')\n"
                    "    INTO v_should_fire\n"
                    "    USING p_old, p_new;\n\n")

        elif dmlEvent=="delete":             
            sql += ("    EXECUTE 'select '||REPLACE(v_alert_definition.sql_expression, 'old.', '$1.')\n"
                    "    INTO v_should_fire\n"
                    "    USING p_old;\n\n")
                        
        sql += "    IF v_should_fire THEN\n"
        
                                                   
        sql += ("      v_alert.level = v_alert_definition.message_level;\n"
                "      v_alert.code = v_alert_definition.alert_name;\n"
                "      v_alert.title = v_alert_definition.alert_title;\n"
                "      v_alert.affected_columns = v_alert_definition.affected_columns;\n"
                "      v_alert.affected_row_count = 1;\n"
                "      EXECUTE 'SELECT '||shared.explode_string_to_select_clause(v_alert_definition.message,'{0}',v_alert_definition.message_inputs)\n"
                "      INTO v_alert.content\n"
                "      USING {0};\n" 
                "      RETURN NEXT v_alert;\n\n"
                "    END IF;\n\n"
                "  END LOOP;\n"                                                        
                "END;\n"
                "$BODY$\n"
                "LANGUAGE plpgsql;\n\n".format(contentRecordPrefixes))
                                           
        return chimpsql.Function(functionName, ALERTS_SCHEMA, returnTypes, sql)


    def getRegisterFunction(self, table, sequence):
        
        functionName = "register_{0}_alert".format(self.sourceName)
        
        params = []
        params.append( ("domain", "character varying", False) )
        params.append( ("alert_enabled", "boolean", False) )
        params.append( ("message_level", "character varying", False) )
        params.append( ("alert_name", "character varying", False) )
        params.append( ("alert_title", "character varying", False) )
        params.append( ("associate_with_record", "boolean", False) )
        
        for field in self.primaryKeyFields:
            params.append( (field.column, field.columnDataType, False ) )
                        
        params.append( ("from_stage", "boolean", False ) )
        params.append( ("from_import", "boolean", False ) )
        params.append( ("from_editable", "boolean", False ) )
        params.append( ("from_mv", "boolean", False ) )
        params.append( ("on_insert", "boolean", False ) )
        params.append( ("on_update", "boolean", False ) )
        params.append( ("on_delete", "boolean", False ) )
        params.append( ("message", "character varying", False ) )
        params.append( ("message_inputs", "character varying", False ) )
        params.append( ("sql_expression", "character varying", False) )
        params.append( ("affected_columns", "character varying", False) )

        valuesClause = ""
        for param in params:
            if not param[2]:
                valuesClause += ",\n      p_{0}".format(param[0])
            else:
                valuesClause += ",\n      string_to_array(p_{0}, ',')".format(param[0])
        
        return chimpsql.Function(functionName, ALERTS_SCHEMA, map(lambda x:x[1], params),
                        ("CREATE OR REPLACE FUNCTION {0}.{1}(\n"
                         "  {2}) RETURNS bigint AS $$\n"
                         "  DECLARE\n"
                         "    v_id bigint;\n"
                         "  BEGIN\n"
                         "    v_id = nextval('{0}.{3}');\n"
                         "    INSERT INTO {0}.{4} (\n"
                         "      id,\n"
                         "      {5})\n"
                         "    VALUES (\n"
                         "      v_id{6});\n"
                         "  RETURN v_id;\n"
                         "  END;\n"
                         "$$ LANGUAGE plpgsql;\n\n").format(ALERTS_SCHEMA, #0
                                                            functionName, #1
                                                            ",\n  ".join(map(lambda x:"p_{0} {1}".format(x[0], x[1]), params)), #2
                                                            sequence.name, #3
                                                            table.name, #4
                                                            ",\n      ".join(map(lambda x:"{0}".format(x[0]), params)),
                                                            valuesClause))   

                  
    def debug(self, appLogger):
        if appLogger is not None:
            appLogger.debug ("  alerts")   
            appLogger.debug ("    sourceType   : {0}".format(self.sourceType))
            appLogger.debug ("    sourceSchema : {0}".format(self.sourceSchema))
            appLogger.debug ("    sourceName   : {0}".format(self.sourceName))
            appLogger.debug ("")
            
    def __init__(self, specificationName, schema, settings, record=None, entity=None):
        
        self.specificationName = specificationName
          
        if record is not None and entity is None:
            self.sourceType = "record"
            self.sourceSchema = schema
            self.sourceName = record.table
                    
            self.fields = list(record.getAllMappedFields())                            
            self.primaryKeyFields = []
            
            for keyColumn in record.primaryKeyColumns:
                for field in record.fields:
                    if field.column is not None:
                        if field.column == keyColumn:
                            self.primaryKeyFields.append(field)

            # Append security/visibility columns?
            if record.editable:
                if schema=="editable":
                    appendExtraColumns=True
                else:
                    appendExtraColumns=False
            else:
                if schema=="import":
                    appendExtraColumns=True
                else:
                    appendExtraColumns=False
            if appendExtraColumns:
                self.fields.append(chimpspec.SpecificationRecordField(None, None,label="Visibility",column="visibility",type="number",mandatory=True,size=4))                    
                self.fields.append(chimpspec.SpecificationRecordField(None, None,label="Security",column="security",type="number",mandatory=True,size=4))                    

            if record.editable and schema=="editable":
                self.fields.append(chimpspec.SpecificationRecordField(None, None,label="Source",column="latest_source",type="text",mandatory=False,size=30))
                
        elif record is None and entity is not None:
            self.sourceType = "entity"
            #TODO: Make alerts work with entities
        
        
            
