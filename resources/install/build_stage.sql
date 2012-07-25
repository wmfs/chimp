-- Create STAGE schema
-- ===================

CREATE SCHEMA stage;
COMMENT ON SCHEMA stage IS 'A schema stage incoming data.';
  
CREATE TABLE stage.server_directory_sources
( source_name character varying(60) NOT NULL,
  directory_path character varying(200) NOT NULL);

CREATE TABLE stage.input_source_types
( type_name character varying(30) NOT NULL,
  type_label character varying(30) NOT NULL);

insert into stage.input_source_types (type_name, type_label) VALUES ('server_directory','Files from a server directory');

CREATE OR REPLACE VIEW stage.all_input_sources AS 
SELECT a.source_type as source_type_name, b.type_label as source_type_label, a.source_name
FROM
(SELECT 'server_directory'::character varying as source_type, source_name
FROM stage.server_directory_sources s) AS a, stage.input_source_types b
WHERE a.source_type = b.type_name;

--insert into stage.input_source_types (type_name, type_label) VALUES ('server_directory','Files from a server directory');


