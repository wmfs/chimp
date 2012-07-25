'''
Created on 4 Mar 2012

@author: Tim.Needham
'''

CTREE_SCHEMA = "ctree"
CALC_SCHEMA = "calc"

import os
import cs
import chimpspec
import chimpsql


class Ctree:
    '''
    classdocs
    '''

    def __init__(self, ctreeTag):
        self.type = "ctree"
        self.taskOrder = 10
        
        
        self.inputAncestorColumn = cs.grabAttribute(ctreeTag,"inputAncestorColumn")
        self.inputDescendantColumn = cs.grabAttribute(ctreeTag,"inputDescendantColumn")
        self.columnSuffix = cs.grabAttribute(ctreeTag,"columnSuffix")

        self.outputDepthColumn = cs.grabAttribute(ctreeTag,"outputDepthColumn")
        if self.outputDepthColumn is None:
            self.outputDepthColumn = "depth"

        self.outputImmediateAncestorColumn = cs.grabAttribute(ctreeTag,"outputImmediateAncestorColumn")
        if self.outputImmediateAncestorColumn is None:
            self.outputImmediateAncestorColumn = "immediate_ancestor"
        
        self.outputRootAncestorColumn = cs.grabAttribute(ctreeTag,"outputRootAncestorColumn")
        if self.outputRootAncestorColumn is None:
            self.outputRootAncestorColumn = "root_ancestor"

        self.outputDescendantCountColumn = cs.grabAttribute(ctreeTag,"outputDescendantCountColumn")
        if self.outputDescendantCountColumn is None:
            self.outputDescendantCountColumn = "descendant_count_column"
        
        self.ancestorColumnName = "ancestor_{0}".format(self.columnSuffix)
        self.descendantColumnName = "descendant_{0}".format(self.columnSuffix)

        self.dataType="bigint"
        self.triggeringColumns = []
        
    def debug(self, appLogger):
        appLogger.debug("    ctree")
        appLogger.debug("      ancestorColumnName           : {0}".format(self.ancestorColumnName))
        appLogger.debug("      descendantColumnName         : {0}".format(self.descendantColumnName))
        appLogger.debug("      inputAncestorColumn          : {0} ({1})".format(self.inputAncestorColumn, self.dataType))
        appLogger.debug("      inputDescendantColumn        : {0} ({1})".format(self.inputDescendantColumn, self.dataType))
        appLogger.debug("      columnSuffix                 : {0}".format(self.columnSuffix))
        appLogger.debug("      outputDepthColumn            : {0}".format(self.outputDepthColumn))
        appLogger.debug("      outputImmediateAncestorColumn: {0}".format(self.outputImmediateAncestorColumn))
        appLogger.debug("      outputRootAncestorColumn     : {0}".format(self.outputRootAncestorColumn))
        appLogger.debug("      outputDescendantCountColumn  : {0}".format(self.outputDescendantCountColumn))

    def getExtraSystemFields(self):
        extraSystemFields = []
        field = chimpspec.SpecificationRecordField(None, None, column=self.outputDepthColumn, type="number", size=5, mandatory=False)
        extraSystemFields.append(field)        
        field = chimpspec.SpecificationRecordField(None, None, column=self.outputImmediateAncestorColumn, type="number", size=12, mandatory=False)
        extraSystemFields.append(field)        
        field = chimpspec.SpecificationRecordField(None, None, column=self.outputRootAncestorColumn, type="number", size=12, mandatory=False)
        extraSystemFields.append(field)                
        field = chimpspec.SpecificationRecordField(None, None, column=self.outputDescendantCountColumn, type="number", size=5, mandatory=False)
        extraSystemFields.append(field)                
        return(extraSystemFields)    
    
    def requiresFile(self):
        return(False)
    
    def getFunctionScript(self, source):
        body= "???\n"
        return(body)  

    def getTriggeringColumns(self):
        columns = []
        columns.append(self.inputAncestorColumn)
        columns.append(self.inputDescendantColumn)
        return(columns)                           


    def getCtreeRegistrationDML(self, specificationName, schemaName, sourceName):
        return chimpsql.DML(("SELECT {0}.register_ctree('{1}', '{2}', '{3}', '{4}', "
                    "'{5}', '{6}', '{7}', '{8}', '{9}', '{10}');\n\n").format(CALC_SCHEMA, 
                                                 specificationName, 
                                                 schemaName,
                                                 sourceName,
                                                 self.inputAncestorColumn,
                                                 self.inputDescendantColumn,
                                                 self.columnSuffix,
                                                 self.outputDepthColumn,
                                                 self.outputImmediateAncestorColumn,
                                                 self.outputRootAncestorColumn,
                                                 self.outputDescendantCountColumn),
                    dropDdl="SELECT {0}.unregister_ctree('{1}','{2}','{3}');\n".format(CALC_SCHEMA, specificationName, schemaName, sourceName))

    def getCtreeTable(self, sourceName): 
        tableName = "{0}_closure".format(sourceName)       
        ddl = ( "CREATE TABLE {0}.{1} (\n"
                "  {2} {3} NOT NULL,\n"
                "  {4} {3} NOT NULL,\n"
                "  {5} integer);\n\n".format(CTREE_SCHEMA, tableName, self.ancestorColumnName, self.dataType, self.descendantColumnName,self.outputDepthColumn) )        
        return chimpsql.Table(tableName, CTREE_SCHEMA, ddl)


    def getAncestorConstraintDML(self, closureTable, sourceSchema, sourceName):
        dml = ("ALTER TABLE {0}.{1}\n"
               "ADD CONSTRAINT {1}_ancestor_fk\n"
               "FOREIGN KEY ({2}) REFERENCES {3}.{4}({5}) DEFERRABLE;\n\n".format(CTREE_SCHEMA, closureTable.name, self.ancestorColumnName, sourceSchema, sourceName, self.inputDescendantColumn))                 
        return chimpsql.DML(dml)

    def getDescendantConstraintDML(self, closureTable, sourceSchema, sourceName):
        dml = ("ALTER TABLE {0}.{1}\n"
               "ADD CONSTRAINT {1}_descendant_fk\n"
               "FOREIGN KEY ({2}) REFERENCES {3}.{4}({5}) DEFERRABLE;\n\n".format(CTREE_SCHEMA, closureTable.name, self.descendantColumnName, sourceSchema, sourceName, self.inputDescendantColumn))                 
        return chimpsql.DML(dml)

    def getAncestorIndex(self, closureTable):
        indexName = "ancestor_{0}_idx".format(closureTable.name)
        return chimpsql.Index(indexName, closureTable.name, CTREE_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} ({3});\n\n".format(indexName, CTREE_SCHEMA, closureTable.name, self.ancestorColumnName))   

    def getDescendantIndex(self, closureTable):
        indexName = "descendant_{0}_idx".format(closureTable.name)
        return chimpsql.Index(indexName, closureTable.name, CTREE_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} ({3});\n\n".format(indexName, CTREE_SCHEMA, closureTable.name, self.descendantColumnName))   

    def getDepthIndex(self, closureTable):
        indexName = "depth_{0}_idx".format(closureTable.name)
        return chimpsql.Index(indexName, closureTable.name, CTREE_SCHEMA,
                     "CREATE INDEX {0} ON {1}.{2} (depth);\n\n".format(indexName, CTREE_SCHEMA, closureTable.name))   

    def getDisconnectFunction(self, closureTable, sourceName):
        functionName = "disconnect_{0}_subtree".format(sourceName)
        return chimpsql.Function(functionName, CTREE_SCHEMA, [self.dataType],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}(p_{3} {5})\n"
                         "  RETURNS void AS\n"
                         "$BODY$\n"
                         "BEGIN\n"
                         "  DELETE FROM {0}.{2}\n"
                         "  WHERE ({3}) IN (SELECT {3} FROM {0}.{2} WHERE {4} = p_{3})\n" 
                         "  AND ({4}) NOT IN (SELECT {3} FROM {0}.{2} WHERE {4} = p_{3});\n" 
                         "END;\n"
                         "$BODY$\n"
                         "LANGUAGE plpgsql;\n\n").format(CTREE_SCHEMA, functionName, closureTable.name, self.descendantColumnName, self.ancestorColumnName, self.dataType))

    def getApplyEdgeFunction(self, closureTable, sourceName):
        functionName = "apply_{0}_edge".format(sourceName)
        return chimpsql.Function(functionName, CTREE_SCHEMA, [self.dataType, self.dataType],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}(p_{4} {5}, p_{3} {5})\n"
                         "  RETURNS void AS\n"
                         "$BODY$\n"
                         "BEGIN\n"
                         "  -- ==================\n" 
                         "  -- DISCONNECT SUBTREE\n" 
                         "  -- ==================\n"
                         "  PERFORM {0}.disconnect_{6}_subtree(p_{3});\n\n"                                             
                         "  -- =================\n"
                         "  -- RECONNECT SUBTREE\n"
                         "  -- =================\n"
                         "  IF p_{4} IS NOT NULL THEN \n" 
                         "    INSERT INTO {0}.{2} ({4}, {3}, depth)\n" 
                         "    SELECT supertree.{4}, subtree.{3}, supertree.depth+subtree.depth+1\n" 
                         "    FROM {0}.{2} AS supertree, {0}.{2} AS subtree\n" 
                         "    WHERE subtree.{4} = p_{3} AND supertree.{3} = p_{4};\n" 
                         "  END IF;\n"
                         "END;\n"
                         "$BODY$\n"
                         "LANGUAGE plpgsql;\n\n").format(CTREE_SCHEMA, #0 
                                                         functionName, #1
                                                         closureTable.name, #2 
                                                         self.descendantColumnName, #3 
                                                         self.ancestorColumnName, #4
                                                         self.dataType, #5
                                                         sourceName)) #6


    def getAllEdgesFunction(self, closureTable, sourceSchema, sourceName):
        functionName = "apply_all_{0}_edges".format(sourceName)
        return chimpsql.Function(functionName, CTREE_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS void AS\n"
                         "$BODY$\n"
                         "  DECLARE\n"
                         "    v_{4} {5};\n" 
                         "    v_{3} {5};\n" 
                         "  BEGIN\n"
                         "    FOR v_{4}, v_{3}\n" 
                         "      IN EXECUTE('select {8}, {9} from {6}.{7} where {8} IS NOT NULL') LOOP\n" 
                         "        PERFORM {0}.apply_{7}_edge(v_{4}, v_{3});\n" 
                         "    END LOOP;\n"
                         "  END;\n"
                         "$BODY$\n"                         
                         "LANGUAGE plpgsql;\n\n").format(CTREE_SCHEMA, #0
                                                         functionName, #1
                                                         closureTable.name, #2 
                                                         self.descendantColumnName, #3 
                                                         self.ancestorColumnName, #4
                                                         self.dataType, #5
                                                         sourceSchema, #6
                                                         sourceName, #7
                                                         self.inputAncestorColumn, #8
                                                         self.inputDescendantColumn)) #9

    def getInsertFunction(self, closureTable, sourceSchema, sourceName):
        functionName = "insert_of_{0}".format(sourceName)
        return chimpsql.Function(functionName, CTREE_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS trigger AS\n"
                         "$BODY$\n"
                         "  BEGIN\n"
                         "    -- Add 'depth 0' link\n"
                         "    INSERT INTO {0}.{2} ({4}, {3}, depth)\n" 
                         "    VALUES (new.{9}, new.{9}, 0);\n\n" 
                         "    IF new.{8} IS NOT NULL THEN\n" 
                         "      IF new.{8} IS DISTINCT FROM new.{9} THEN\n" 
                         "        PERFORM {0}.apply_{7}_edge(new.{8}, new.{9});\n" 
                         "      END IF;\n"
                         "    END IF;\n\n"
                         "    RETURN new;\n"
                         "  END;\n"
                         "$BODY$\n"                         
                         "LANGUAGE plpgsql;\n\n").format(CTREE_SCHEMA, #0
                                                         functionName, #1
                                                         closureTable.name, #2 
                                                         self.descendantColumnName, #3 
                                                         self.ancestorColumnName, #4
                                                         self.dataType, #5
                                                         sourceSchema, #6
                                                         sourceName, #7
                                                         self.inputAncestorColumn, #8
                                                         self.inputDescendantColumn)) #9

    def getInsertTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "e_closure_insert_for_{0}_ctree".format(sourceName)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER INSERT\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, sourceSchema, sourceName, CTREE_SCHEMA, triggerFunction.name))


    def getDeleteFunction(self, closureTable, sourceSchema, sourceName):
        functionName = "delete_of_{0}".format(sourceName)
        return chimpsql.Function(functionName, CTREE_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS trigger AS\n"
                         "$BODY$\n"
                         "  BEGIN\n"
                         "    DELETE FROM {0}.{2}\n" 
                         "    WHERE {4} = old.{9}\n" 
                         "    OR {3} = old.{9};\n" 
                         "    RETURN old;\n"
                         "  END;\n"
                         "$BODY$\n"                         
                         "LANGUAGE plpgsql;\n\n").format(CTREE_SCHEMA, #0
                                                         functionName, #1
                                                         closureTable.name, #2 
                                                         self.descendantColumnName, #3 
                                                         self.ancestorColumnName, #4
                                                         self.dataType, #5
                                                         sourceSchema, #6
                                                         sourceName, #7
                                                         self.inputAncestorColumn, #8
                                                         self.inputDescendantColumn)) #9

    def getDeleteTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "e_closure_delete_for_{0}_ctree".format(sourceName)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "BEFORE DELETE\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, sourceSchema, sourceName, CTREE_SCHEMA, triggerFunction.name))



    def getUpdateFunction(self, closureTable, sourceSchema, sourceName):
        functionName = "update_of_{0}".format(sourceName)
        return chimpsql.Function(functionName, CTREE_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS trigger AS\n"
                         "$BODY$\n"
                         "  BEGIN\n"
                         "    PERFORM {0}.apply_{7}_edge(new.{8},new.{9});\n"
                         "    RETURN new;\n"
                         "  END;\n"
                         "$BODY$\n"                         
                         "LANGUAGE plpgsql;\n\n").format(CTREE_SCHEMA, #0
                                                         functionName, #1
                                                         closureTable.name, #2 
                                                         self.descendantColumnName, #3 
                                                         self.ancestorColumnName, #4
                                                         self.dataType, #5
                                                         sourceSchema, #6
                                                         sourceName, #7
                                                         self.inputAncestorColumn, #8
                                                         self.inputDescendantColumn)) #9

    def getUpdateTrigger(self, sourceSchema, sourceName, triggerFunction):
        triggerName = "e_closure_update_for_{0}_ctree".format(sourceName)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, sourceSchema,
                       ("CREATE TRIGGER {0}\n"
                        "BEFORE UPDATE\n"
                        "OF {5},{6}\n"
                        "ON {1}.{2}\n"
                        "FOR EACH ROW\n"
                        "WHEN (old.{5} IS DISTINCT FROM new.{5} OR old.{6} IS DISTINCT FROM new.{6})\n"
                        "EXECUTE PROCEDURE {3}.{4}();\n\n").format(triggerName, #0
                                                                   sourceSchema, #1
                                                                   sourceName, #2
                                                                   CTREE_SCHEMA, #3
                                                                   triggerFunction.name, #4
                                                                   self.inputAncestorColumn, #5
                                                                   self.inputDescendantColumn)) #6


    def getType(self, sourceSchema, sourceName):
        typeName = "{0}_node_placement" .format(sourceName)
        return chimpsql.Type(typeName, CTREE_SCHEMA, 
                            ("CREATE TYPE {0}.{1} AS\n"
                             "(depth integer,\n"
                             " immediate_{3} {4},\n"
                             " root_{3} {4},\n"
                             " descendant_count integer);\n\n").format(CTREE_SCHEMA, #0
                                                                      typeName, #1
                                                                      self.descendantColumnName, #2 
                                                                      self.ancestorColumnName, #3
                                                                      self.dataType)) #4
                            

    def getInfoFunction(self, closureTable, sourceSchema, sourceName):
        functionName = "get_{0}_node_placement".format(sourceName)
        return chimpsql.Function(functionName, CTREE_SCHEMA, [self.dataType],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}(p_{4} {5})\n"
                         "  RETURNS {0}.{7}_node_placement AS\n"
                         "$BODY$\n"
                         "  DECLARE\n"
                         "    v_placement {0}.{7}_node_placement;\n"
                         "  BEGIN\n"
                         "    SELECT max(depth)\n"
                         "    INTO v_placement.depth\n"
                         "    FROM {0}.{2}\n"
                         "    WHERE {3} = p_{4};\n\n"
                         "    IF v_placement.depth IS NOT NULL THEN\n"
                         "      IF v_placement.depth = 0 THEN\n"
                         "        v_placement.immediate_{4} = p_{4};\n"
                         "        v_placement.root_{4} = p_{4};\n"
                         "      ELSE\n"
                         "        SELECT {4}\n"
                         "        INTO v_placement.immediate_{4}\n"
                         "        FROM {0}.{2}\n"
                         "        WHERE {3} = p_{4}\n"
                         "        AND depth = 1;\n\n"                
                         "        SELECT {4}\n"
                         "        INTO v_placement.root_{4}\n"
                         "        FROM {0}.{2}\n"
                         "        WHERE {3} = p_{4}\n"
                         "        AND depth = v_placement.depth;\n"
                         "      END IF;\n\n"
                         "      SELECT count(*)\n"
                         "      INTO v_placement.descendant_count\n"
                         "      FROM {0}.{2}\n"
                         "      WHERE {4} = p_{4}\n"
                         "      AND depth > 0;\n"
                         "    END IF;\n\n"
                         "    RETURN v_placement;\n\n"
                         "  END;\n"
                         "$BODY$\n"
                         "LANGUAGE plpgsql;\n\n").format(CTREE_SCHEMA, #0
                                                         functionName, #1
                                                         closureTable.name, #2 
                                                         self.descendantColumnName, #3 
                                                         self.ancestorColumnName, #4
                                                         self.dataType, #5
                                                         sourceSchema, #6
                                                         sourceName, #7
                                                         self.inputAncestorColumn, #8
                                                         self.inputDescendantColumn, #9
                                                         self.columnSuffix)) #10
    def getCtreeQueueTable(self, sourceName): 
        tableName = "{0}_ctree_queue".format(sourceName)       
        ddl = ( "CREATE TABLE {0}.{1} (\n"
                "  {2} {3} PRIMARY KEY);\n\n".format(CALC_SCHEMA, tableName, self.columnSuffix, self.dataType))        
        return chimpsql.Table(tableName, CALC_SCHEMA, ddl)

    def getCtreeQueueFunction(self, sourceName):
        functionName = "add_to_{0}_ctree_queue".format(sourceName)
        return chimpsql.Function(functionName, CTREE_SCHEMA, [],
                        ("CREATE OR REPLACE FUNCTION {0}.{1}()\n"
                         "  RETURNS trigger AS\n"
                         "$BODY$\n"
                         "DECLARE\n"
                         "  v_exists BOOLEAN;\n"
                         "  v_{3} bigint;\n"
                         "BEGIN\n"
                         "  IF TG_OP = 'DELETE' THEN\n"
                         "    v_uprn = old.descendant_uprn;\n"
                         "  ELSE\n"
                         "    v_uprn = new.descendant_uprn;\n"
                         "  END IF;\n"
                         "  SELECT exists(SELECT 1 FROM {0}.{2}_ctree_queue WHERE {3}= v_uprn)\n"
                         "  INTO v_exists;\n"
                         "  IF NOT v_exists THEN\n"
                         "    INSERT INTO {0}.{2}_ctree_queue(\n"
                         "      {3})\n"
                         "    VALUES (\n"
                         "      v_uprn);\n"
                         "  END IF;\n"
                         "  IF TG_OP = 'DELETE' THEN\n"
                         "    RETURN old;\n"
                         "  ELSE\n"
                         "    RETURN new;\n"
                         "  END IF;\n"                                    
                         "END;\n"
                         "$BODY$\n"
                         "LANGUAGE plpgsql;\n\n").format(CALC_SCHEMA, functionName, sourceName, self.inputDescendantColumn, self.descendantColumnName))

    def getCtreeQueueInsertTrigger(self, sourceName, triggerFunction):
        triggerName = "o_add_{0}_insert_to_ctree_queue".format(sourceName)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, CALC_SCHEMA,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER INSERT\n"
                        "ON {4}.{1}_closure\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {2}.{3}();\n\n").format(triggerName, sourceName, CALC_SCHEMA, triggerFunction.name, CTREE_SCHEMA))

    def getCtreeQueueDeleteTrigger(self, sourceName, triggerFunction):
        triggerName = "o_add_{0}_delete_to_ctree_queue".format(sourceName)
        return chimpsql.Trigger(triggerName, sourceName, triggerFunction.name, CALC_SCHEMA,
                       ("CREATE TRIGGER {0}\n"
                        "AFTER DELETE\n"
                        "ON {4}.{1}_closure\n"
                        "FOR EACH ROW\n"
                        "EXECUTE PROCEDURE {2}.{3}();\n\n").format(triggerName, sourceName, CALC_SCHEMA, triggerFunction.name, CTREE_SCHEMA))
        
    def generateEnableAndRecreateScript(self, repositoryPath, specificationName, closureTable, sourceSchema, sourceName):
        filename = os.path.join(repositoryPath,"scripts", "generated", "specification files", specificationName, "sql", "ctree","{0}_enable_and_recreate.sql".format(sourceName))
        insertTriggerName = "e_closure_insert_for_{0}_ctree".format(sourceName)
        updateTriggerName = "e_closure_update_for_{0}_ctree".format(sourceName)
        deleteTriggerName = "e_closure_delete_for_{0}_ctree".format(sourceName)
        allEdgesFunctionName = "apply_all_{0}_edges".format(sourceName)
        ctreeFile = open(filename,"w")                
        ctreeFile.write("ALTER TABLE {0}.{1} ENABLE TRIGGER {2};\n".format(sourceSchema, sourceName, insertTriggerName))
        ctreeFile.write("ALTER TABLE {0}.{1} ENABLE TRIGGER {2};\n".format(sourceSchema, sourceName, updateTriggerName))
        ctreeFile.write("ALTER TABLE {0}.{1} ENABLE TRIGGER {2};\n".format(sourceSchema, sourceName, deleteTriggerName))
        ctreeFile.write("TRUNCATE TABLE {0}.{1};\n".format(CTREE_SCHEMA, closureTable.name))                
        line = "INSERT INTO {0}.{1} ({2}, {3}, depth) SELECT {4}, {4}, 0 FROM {5}.{6};\n".format(CTREE_SCHEMA, closureTable.name, self.ancestorColumnName, self.descendantColumnName, self.inputDescendantColumn, sourceSchema, sourceName)                    
        ctreeFile.write(line)                            
        indexDdl="CREATE INDEX ancestor_{0}_idx ON {1}.{0} ({2});\n".format(closureTable.name, CTREE_SCHEMA, self.ancestorColumnName)
        ctreeFile.write(indexDdl)                    
        indexDdl="CREATE INDEX descendant_{0}_idx ON {1}.{0} ({2});\n".format(closureTable.name, CTREE_SCHEMA, self.descendantColumnName)
        ctreeFile.write(indexDdl)                    
        indexDdl="CREATE INDEX depth_{0}_idx ON {1}.{0} (depth);\n".format(closureTable.name, CTREE_SCHEMA)
        ctreeFile.write(indexDdl)
        ctreeFile.write("SELECT {0}.{1}();\n".format(CTREE_SCHEMA, allEdgesFunctionName))

        ctreeFile.write("TRUNCATE TABLE {0}.{1}_ctree_queue;\n".format(CALC_SCHEMA, sourceName))                
        line = "INSERT INTO {4}.{1}_ctree_queue ({2}) SELECT {2} FROM {3}.{1};\n".format(CTREE_SCHEMA, sourceName, self.inputDescendantColumn, sourceSchema,CALC_SCHEMA)                    
        ctreeFile.write(line)                            
        insertTriggerName = "o_add_{0}_insert_to_ctree_queue".format(sourceName)
        deleteTriggerName = "o_add_{0}_delete_to_ctree_queue".format(sourceName)   
        ctreeFile.write("ALTER TABLE {0}.{1}_closure ENABLE TRIGGER {2};\n".format(CTREE_SCHEMA, sourceName, insertTriggerName))
        ctreeFile.write("ALTER TABLE {0}.{1}_closure ENABLE TRIGGER {2};\n".format(CTREE_SCHEMA, sourceName, deleteTriggerName))
        
        
        ctreeFile.close()        

    def generateDisableScript(self, repositoryPath, specificationName, closureTable, sourceSchema, sourceName):
        filename = os.path.join(repositoryPath,"scripts", "generated", "specification files", specificationName, "sql", "ctree", "{0}_disable.sql".format(sourceName))
        insertTriggerName = "e_closure_insert_for_{0}_ctree".format(sourceName)
        updateTriggerName = "e_closure_update_for_{0}_ctree".format(sourceName)
        deleteTriggerName = "e_closure_delete_for_{0}_ctree".format(sourceName)
        ctreeFile = open(filename,"w")
        ctreeFile.write("ALTER TABLE {0}.{1} DISABLE TRIGGER {2};\n".format(sourceSchema, sourceName, insertTriggerName))
        ctreeFile.write("ALTER TABLE {0}.{1} DISABLE TRIGGER {2};\n".format(sourceSchema, sourceName, updateTriggerName))
        ctreeFile.write("ALTER TABLE {0}.{1} DISABLE TRIGGER {2};\n".format(sourceSchema, sourceName, deleteTriggerName))
        insertTriggerName = "o_add_{0}_insert_to_ctree_queue".format(sourceName)
        deleteTriggerName = "o_add_{0}_delete_to_ctree_queue".format(sourceName)   
        ctreeFile.write("ALTER TABLE {0}.{1}_closure DISABLE TRIGGER {2};\n".format(CTREE_SCHEMA, sourceName, insertTriggerName))
        ctreeFile.write("ALTER TABLE {0}.{1}_closure DISABLE TRIGGER {2};\n".format(CTREE_SCHEMA, sourceName, deleteTriggerName))
                                        
        ctreeFile.close()


def processCtrees(queue, supportConnection, supportCursor, loopConnection, dataConnection, dataCursor, settings, taskId, processLimit, args):
    # Init
    lineCount = 0
    successCount = 0
    exceptionCount=0
    errorCount=0
    warningCount=0
    noticeCount=0
    ignoredCount=0   
    appLogger = settings.appLogger
            
    sourceSchema = args["inputSourceSchema"]
    sourceName = args["inputSourceName"]
    ancestorColumnName = args["ancestorColumnName"] 
    descendantColumnName = args["descendantColumnName"]
    columnSuffix = args["columnSuffix"] 
    depthColumnName = args["depthColumnName"] 
    immediateAncestorColumnName = args["immediateAncestorColumnName"] 
    rootAncestorColumnName = args["rootAncestorColumnName"] 
    descendantCountColumnName = args["descendantCountColumnName"] 

    
    # Publish count
    queue.startTask(taskId, True)
    sql = "select count(*) from {0}.{1}_ctree_queue".format(CALC_SCHEMA, sourceName)
    supportCursor.execute(sql)
    ctreeCount = supportCursor.fetchone()[0]
    queue.setScanResults(taskId, ctreeCount)
    appLogger.info(" |   ctreeCount : {0}".format(ctreeCount))

    # build update SQL
    updateSql = "update {0}.{1} set {2}=%s,{3}=%s,{4}=%s,{5}=%s where {6}=%s".format(sourceSchema,sourceName,depthColumnName,immediateAncestorColumnName,rootAncestorColumnName,descendantCountColumnName,descendantColumnName)
    appLogger.info(" |   updateSql  : {0}".format(updateSql))

    # build delete SQL
    deleteSql = "delete from {0}.{1}_ctree_queue where {2}=%s".format(CALC_SCHEMA,sourceName, descendantColumnName)
    appLogger.info(" |   deleteSql  : {0}".format(deleteSql))
    
    # Establish main loop
    loopSql = "select a.{0},{3}.get_{2}_node_placement({0}) from {1}.{2}_ctree_queue as a".format(descendantColumnName, CALC_SCHEMA, sourceName,CTREE_SCHEMA)
    appLogger.info(" |   loopSql    : {0}".format(loopSql))   
    loopCursor = loopConnection.makeCursor(None, False, False)
    loopCursor.execute(loopSql)
    
    for record in loopCursor:
        successCount+=1
        if successCount%1000 ==0:
            queue.setTaskProgress(taskId, successCount, 0, 0, 0, 0, 0)
        
        pk = record[0]
        placement = record[1][1:-1].split(",")
        depth = placement[0]
        immediateAncestor = placement[1]
        rootAncestor = placement[2]
        descendantCount = placement[3]
        
        # update source columns
#        print(updateSql)
#        print((depth,immediateAncestor,rootAncestor,descendantCount,pk))
        dataCursor.execute(updateSql, (depth,immediateAncestor,rootAncestor,descendantCount,pk))
        # Delete from queue
        dataCursor.execute(deleteSql, (pk,))

 
 
    loopCursor.close()
    queue.finishTask(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
           
#    # Create new pins SQL
#    newPinsSql = "select nextval('{0}.{1}_seq') as pin_id,{2},{3} from {4}.{5} where ({6}=%s) AND ({7} IS NOT NULL AND {8} IS NOT NULL)".format(PINHEAD_SCHEMA,pinName,inputIdColumn,inputColumnList,inputSchema,inputSource,inputIdColumn, inputXColumn, inputYColumn)
#    appLogger.debug("  newPinsSql        : {0}".format(newPinsSql))
#    newPinCursor = dataConnection.makeCursor(None, False, True)
#    
#    # Create insert DML    
#    placeHolders="%s"
#    i = 0
#    while i<outputColumnList.count(","):
#        placeHolders += ",%s"
#        i += 1    
#    insert = "insert into {0}.{1}({2}) values ({3})".format(PINHEAD_SCHEMA, pinName, outputColumnList,placeHolders)
#    appLogger.debug("  insertSql         : {0}".format(insert))
#
#    # Create delete pin DML
#    deleteExistingPins = "delete from {0}.{1} where source_id=%s".format(PINHEAD_SCHEMA, pinName)
#
#    # Create delete from queue DML
#    deleteFromQueue = "delete from {0}.{1}_pins_queue where source_id=%s".format(CALC_SCHEMA, pinName)
#
#
#    loopCursor.execute(loopSql)
#    recordCount=0
#    for record in loopCursor:
#        
#        recordCount+=1
#        if recordCount%1000 ==0:
#            queue.setTaskProgress(taskId, recordCount, 0, 0, 0, 0, 0)
#            
#        # Delete any pins that might be there already
#        dataCursor.execute(deleteExistingPins, (record[0],))
#        
#        # Insert new pins
#        newPinCursor.execute(newPinsSql, (record[0],))
#        for newPin in newPinCursor:
#            output = outputMethod(dataCursor, newPin)
#            dataCursor.execute(insert, output)
#        
#        # Delete from queue
#        dataCursor.execute(deleteFromQueue, (record[0],))
#        
#        lineCount += 1
#
##        output = outputMethod(dataCursor, record)
##        dataCursor.execute(insert, output)
##    
#    queue.finishTask(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)        
#    supportConnection.connection.commit()
#    




#    
#    
#    for thisRecord in specification.records:            
#        if thisRecord.table == sourceTable:                
#            for thisPin in thisRecord.pins:
#                if thisPin.name==pinName: 
#
#                    # Construct SQL/DML
#                    # =================
#                    dmlColumnCount=7
#                    selectList=[]
#                    paramList = []
#
#                    selectList.append("a.id")                            
#                    for thisPrimaryKeyColumn in thisRecord.primaryKeyColumns:
#                        selectList.append("a."+thisPrimaryKeyColumn)
#                        dmlColumnCount = dmlColumnCount + 1
#                                                    
#                    for additionalColumn in thisPin.additionalPinColumns:
#                        selectList.append("a."+additionalColumn)
#                        dmlColumnCount = dmlColumnCount + 1
#
#                    selectList.append("a." + thisPin.xColumn)
#                    selectList.append("a." + thisPin.yColumn)
#
#                    for thisGenerator in thisPin.iconGenerators:
#                        dmlColumnCount = dmlColumnCount + 2
#                        if thisGenerator.simpleMappingSourceColumn is not None:
#                            i=0
#                            for thisColumn in selectList:
#                                if thisColumn == "a.%s" %(thisGenerator.simpleMappingSourceColumn):
#                                    thisGenerator.setColumnIndex(i)
#                                i=i+1
#                    
#                    selectList.append("p.x" )
#                    selectList.append("p.y")                        
#                    selectList.append("coalesce(a.visibility,%s.get_%s_default_visibility())" %(sourceSchema,sourceTable))
#                    selectList.append("coalesce(a.security,%s.get_%s_default_security())" %(sourceSchema,sourceTable))
#                     
#                    sql = "select " + cs.delimitedStringList(selectList, ",")
#                    sql = sql + " from %s.%s AS a" %(sourceSchema, sourceTable)                                                                                 
#                    sql=sql+" LEFT JOIN pinhead.%s as p ON(a.id=p.id)" %(pinName)
#                    
#                    if lastSynchronized is not None:
#                        sql=sql+" WHERE a.%s_pin_modified > " %(pinName)
#                        sql=sql+"%s"
#                        loopCursor.execute(sql, (lastSynchronized,))
#                    else:
#                        loopCursor.execute(sql)
#
#                    # Build DML
#                    dml = "select pinhead.synchronize_%s_pin(" %(pinName)
#                    for i in range(0,dmlColumnCount):
#                        if i>0:
#                            dml=dml+","
#                        dml=dml+"%s"
#                    dml=dml+")"
#                    
#                    # Main loop                                                
#                    lineCount = 0
#                    successCount = 0
#                    exceptionCount=0
#                    errorCount=0
#                    warningCount=0
#                    noticeCount=0
#                    ignoredCount=0   
#                    
#                                           
#                    for data in loopCursor:                            
#                        if lineCount % 1000 == 0:                
#                            queue.setTaskProgress(taskId, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)
#                        lineCount = lineCount + 1                            
#                        
#                        workingData = list(data)
#                        
#                        for thisGenerator in thisPin.iconGenerators:             
#                            
#                            if thisGenerator.simpleMappingSourceColumn is not None:
#                                                   
#                                sourceColumnData = data[thisGenerator.simpleMappingSourceColumnIndex]                                
#                                
#                                if str(sourceColumnData) in thisGenerator.mapping:
#                                    # Value can be mapped to a filename/priority
#                                    mappedValues = thisGenerator.mapping[str(sourceColumnData)]
#                                    workingData.append(mappedValues[0])
#                                    workingData.append(mappedValues[1])
#                                else:
#                                    # Not in mapping list, but has defaults so...
#                                    if thisGenerator.simpleMappingUnmappedFilename is not None and thisGenerator.simpleMappingUnmappedPriority is not None:
#                                        workingData.append(thisGenerator.simpleMappingUnmappedFilename)
#                                        workingData.append(thisGenerator.simpleMappingUnmappedPriority)
#                                    else:
#                                        # Not mapped, no defaults... clear it off.
#                                        workingData.append(None)
#                                        workingData.append(None)                                            
#                            else:                                    
#                                workingData.append(thisGenerator.fixedFilename)
#                                workingData.append(None)                           
#
#                        
#                        try:
#                            # Apply DML statement
#                            dataCursor.execute(dml, tuple(workingData))                            
#                            successCount = successCount + 1
#                            
#                        except Exception as detail:
#                            try:
#                                exceptionCount = exceptionCount + 1
##                                    dataConnection.connection.rollback()                                    
#                                queue.addTaskMessage(taskId, "%s.%s" %(sourceSchema,sourceTable), lineCount, "exception", "EXP", "Exception attempting synchronization of %s pin" %(pinName), None, 1, "Message:\n%s\n\nData:\n%s"  %(str(detail), str(data)))
#
#                            except Exception as subDetail:
#                                print ("Error")
#                                print (detail)
#                                print ("Failed to log error:")
#                                print (subDetail)
#                                raise
#
#                    #chimpqueue.finishTask(supportConnection, supportCursor, taskId, True, True, successCount, exceptionCount, errorCount, warningCount, noticeCount, ignoredCount)                    
#                                                                                

    return( (successCount, exceptionCount, errorCount, warningCount, ignoredCount, noticeCount) )



# ----------------------------------------------------------------------------------

#        return chimpsql.Table(self.name, PINHEAD_SCHEMA,
#                     ("CREATE TABLE {0}.{1} (\n"
#                        "  id bigint primary key{2}{3},\n"
#                        "  x int not null,\n"
#                        "  y int not null{4},\n"
#                        "  visibility smallint,\n"
#                        "  security smallint,\n"
#                        "  created timestamp with time zone not null default current_timestamp,\n"
#                        "  modified timestamp with time zone not null default current_timestamp)\n"
#                        "WITH (OIDS=TRUE);\n\n").format(PINHEAD_SCHEMA,self.name,
#                                                        self._getColumnDefsSQL(record.getPrimaryKeyFields()),
#                                                        "".join(map(lambda icon: (",\n"
#                                                                                  "  {0}_filename character varying(60),\n"
#                                                                                  "  {0}_priority integer default 100").format(icon.name), "???")),
#                                                        self._getColumnDefsSQL(record.getAdditionalPinheadFields(self))))   
        
#    def getPinheadPrimaryKeyColumnsIndex(self,  record, table):
#        indexName = "{0}_pk_columns".format(self.name)
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE UNIQUE INDEX {0} ON {1}.{2} ({3});\n".format(indexName, PINHEAD_SCHEMA, table.name, ",".join(record.primaryKeyColumns)))
    
#    def getPinheadModifiedIndex(self,  record, table):
#        indexName = "{0}_modified".format(self.name)                            
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE INDEX {0} ON {1}.{2} (modified);\n".format(indexName, PINHEAD_SCHEMA, table.name))
#    
#    def getPinheadVisibilityIndex(self,  record, table):
#        indexName = "{0}_visibility".format(self.name)                            
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE INDEX {0} ON {1}.{2} (visibility);\n".format(indexName, PINHEAD_SCHEMA, table.name))
#    
#    def getPinheadSecurityIndex(self,  record, table):
#        indexName = "{0}_security".format(self.name)
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE INDEX {0} ON {1}.{2} (security);\n".format(indexName, PINHEAD_SCHEMA, table.name))
#    
#    def getPinheadPinIndexIndex(self,  record, table):
#        indexName = "{0}_pin_idx".format(self.name)
#        return chimpsql.Index(indexName, table.name, table.schema,
#                     "CREATE INDEX {0} ON {1}.{2} USING gist (pin);\n".format(indexName, PINHEAD_SCHEMA, table.name))         
#    
    
