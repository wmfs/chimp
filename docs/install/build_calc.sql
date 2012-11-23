
-- Create CALC schema
-- ===================

CREATE SCHEMA calc;
COMMENT ON SCHEMA calc IS 'A schema to help process the various types of auto-calculation';


CREATE TABLE calc.solr_server_registry(
name character varying(200) PRIMARY KEY,
server_url character varying(200) NOT NULL,
server_version character varying(200) NOT NULL,
connection_name character varying(200) NOT NULL,
capabilities character varying(4000),
field_count integer NOT NULL);

CREATE TABLE calc.solr_document_registry(
document_name character varying(200),
server_name character varying(200),
specification_name character varying(200) NOT NULL,
source_schema  character varying(200) NOT NULL,
source_name character varying(200) NOT NULL,
PRIMARY KEY (document_name, specification_name));

CREATE INDEX solr_server_specification_idx ON calc.solr_document_registry(specification_name);

CREATE OR REPLACE FUNCTION calc.register_solr_server(
  p_name character varying,
  p_server_url character varying,
  p_server_version character varying,
  p_connection_name character varying,
  p_capabilities character varying,
  p_field_count integer
) RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO calc.solr_server_registry(
      name,
      server_url,
      server_version,
      connection_name,
      capabilities,
      field_count)
    VALUES (
      p_name,
      p_server_url,
      p_server_version,
      p_connection_name,
      p_capabilities,
      p_field_count);
  END;
$BODY$
LANGUAGE 'plpgsql';        

CREATE OR REPLACE FUNCTION calc.register_solr_document(
  p_document_name character varying,
  p_server_name character varying,
  p_specification_name character varying,
  p_source_schema character varying,
  p_source_name character varying
) RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO calc.solr_document_registry(
      document_name,
      server_name,
      specification_name,
      source_schema,
      source_name)
    VALUES (
      p_document_name,
      p_server_name,
      p_specification_name,
      p_source_schema,
      p_source_name);
  END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE VIEW calc.solr_server_document_view AS
SELECT d.document_name,
        d.source_schema,
        d.source_name,
        d.specification_name,
        s.name AS server_name,
        s.server_url,
        s.field_count
FROM calc.solr_server_registry AS s JOIN calc.solr_document_registry AS d ON (s.name = d.server_name);

CREATE OR REPLACE FUNCTION calc.unregister_solr_server(
  p_server_name character varying
) RETURNS void AS
$BODY$
  BEGIN 
    DELETE FROM calc.solr_server_registry
    WHERE name = p_server_name;
  END; 
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION calc.unregister_solr_document(
  p_document_name character varying
) RETURNS void AS
$BODY$
  DECLARE
    v_server character varying (30);
  BEGIN 
    
    SELECT server_name
    INTO v_server
    FROM calc.solr_document_registry
    WHERE document_name = p_document_name;

    IF v_server IS NOT NULL THEN
      EXECUTE 'DELETE FROM solr.'||v_server||' WHERE document_type=$1'
      USING p_document_name;
    END IF;

    DELETE FROM calc.solr_document_registry
    WHERE document_name = p_document_name;    
  END; 
$BODY$
LANGUAGE 'plpgsql';

CREATE TABLE calc.custom_registry
(
  specification_name character varying(200) NOT NULL,
  source_schema character varying(200) NOT NULL,
  source_name character varying(200) NOT NULL,
  output_column_list character varying(4000) NOT NULL,
  seq integer NOT NULL,
  input_column_list character varying(4000) NOT NULL,
  PRIMARY KEY (specification_name,source_schema,source_name,output_column_list));
  
CREATE INDEX custom_specification ON calc.custom_registry(specification_name);

CREATE OR REPLACE FUNCTION calc.register_custom(
  p_specification_name character varying, 
  p_source_schema character varying, 
  p_source_name character varying, 
  p_output_column_list character varying, 
  p_seq integer,
  p_input_column_list character varying)  
  RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO calc.custom_registry(
      specification_name,
      source_schema,
      source_name,
      output_column_list,
      seq,
      input_column_list)
    VALUES
     (p_specification_name, 
      p_source_schema, 
      p_source_name, 
      p_output_column_list, 
      p_seq,
      p_input_column_list);
  END;
$BODY$
  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION calc.unregister_custom(p_specification_name character varying, p_source_schema character varying, p_source_name character varying, p_output_column_list character varying)
  RETURNS void AS
$BODY$
  BEGIN 
    DELETE FROM calc.custom_registry
    WHERE specification_name=p_specification_name
      AND source_schema=p_source_schema
      AND source_name=p_source_name
      AND output_column_list=p_output_column_list;
  END; 
$BODY$
  LANGUAGE plpgsql;
  
CREATE TABLE calc.ctree_registry
(
  specification_name character varying(200) NOT NULL,
  source_schema character varying(200) NOT NULL,
  source_name character varying(200) NOT NULL,
  ancestor_column_name character varying(200) NOT NULL,
  descendant_column_name character varying(200) NOT NULL,
  column_suffix  character varying(30) NOT NULL,
  depth_column_name character varying(200) NOT NULL,
  immediate_ancestor_column_name character varying(200) NOT NULL,
  root_ancestor_column_name character varying(200) NOT NULL,
  descendant_count_column character varying(200) NOT NULL,
  PRIMARY KEY (specification_name,source_schema,source_name));
  
CREATE INDEX ctree_specification ON calc.ctree_registry(specification_name);

CREATE OR REPLACE FUNCTION calc.register_ctree(
  p_specification_name character varying, 
  p_source_schema character varying, 
  p_source_name character varying, 
  p_ancestor_column_name character varying,
  p_descendant_column_name character varying,
  p_column_suffix character varying,
  p_depth_column_name character varying,
  p_immediate_ancestor_column_name character varying,
  p_root_ancestor_column_name character varying,
  p_descendant_count_column character varying)  
  RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO calc.ctree_registry(
      specification_name,
      source_schema,
      source_name,
      ancestor_column_name,
      descendant_column_name,
      column_suffix,
      depth_column_name,
      immediate_ancestor_column_name,
      root_ancestor_column_name,
      descendant_count_column)
    VALUES
     (p_specification_name, 
      p_source_schema, 
      p_source_name, 
      p_ancestor_column_name,
      p_descendant_column_name,
      p_column_suffix,
      p_depth_column_name,
      p_immediate_ancestor_column_name,
      p_root_ancestor_column_name,
      p_descendant_count_column);
  END;
$BODY$
  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION calc.unregister_ctree(p_specification_name character varying, p_source_schema character varying, p_source_name character varying)
  RETURNS void AS
$BODY$
  BEGIN 
    DELETE FROM calc.ctree_registry
    WHERE specification_name=p_specification_name
      AND source_schema=p_source_schema
      AND source_name=p_source_name;
  END; 
$BODY$
  LANGUAGE plpgsql;


CREATE TABLE calc.pin_registry(
pin_name character varying(200) PRIMARY KEY,
specification_name character varying(200) NOT NULL,
label character varying(200) NOT NULL,
description character varying(4000),
input_id_column character varying(200) NOT NULL,
input_x_column character varying(200) NOT NULL,
input_y_column character varying(200) NOT NULL,
input_schema character varying(200) NOT NULL,
input_source_name character varying(200) NOT NULL,
input_column_list character varying(4000) NOT NULL,
output_column_list character varying(4000) NOT NULL,
where_clause character varying(4000));

CREATE INDEX pin_registry_source ON calc.pin_registry (input_schema, input_source_name);

CREATE OR REPLACE FUNCTION calc.register_pin(
  p_pin_name character varying,
  p_specification_name character varying,
  p_label character varying,
  p_description character varying,
  p_input_id_column character varying,
  p_input_x_column character varying,
  p_input_y_column character varying,
  p_input_schema character varying,
  p_input_source_name character varying,
  p_input_column_list character varying,
  p_output_column_list character varying,  
  p_where_clause character varying
) RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO calc.pin_registry(
      pin_name,
      specification_name,
      label,
      description,
      input_id_column,
      input_x_column,
      input_y_column,
      input_schema,
      input_source_name,
      input_column_list,
      output_column_list,
      where_clause)
    VALUES
     (p_pin_name,
      p_specification_name,
      p_label,
      p_description,
      p_input_id_column,
      p_input_x_column,
      p_input_y_column,
      p_input_schema,
      p_input_source_name,
      p_input_column_list,
      p_output_column_list,
      p_where_clause);
  END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION calc.unregister_pin(
  p_pin_name character varying
) RETURNS void AS
$BODY$
  BEGIN 
    DELETE FROM calc.pin_registry
    WHERE pin_name = p_pin_name;
  END; 
$BODY$
LANGUAGE 'plpgsql';

