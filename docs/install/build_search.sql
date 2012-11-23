
-- Create SEARCH schema
-- =====================

CREATE SCHEMA search;
COMMENT ON SCHEMA search IS 'A schema to store objects for all things search.';

CREATE TABLE search.domains(
domain_name character varying(200) PRIMARY KEY,
synchronization_enabled boolean NOT NULL,
config_location character varying(30) NOT NULL);

CREATE OR REPLACE FUNCTION search.register_domain(p_domain_name character varying,
                                                    p_synchronization_enabled boolean,
                                                    p_config_location character varying) RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO search.domains(
      domain_name,
      synchronization_enabled,
      config_location)
    VALUES
     (p_domain_name,
      p_synchronization_enabled,
      p_config_location);
  END; 
$BODY$
LANGUAGE 'plpgsql';


CREATE TABLE search.domain_sources(
domain_name character varying(200) NOT NULL,
source_type character varying(30) NOT NULL,
source_schema character varying(200) NOT NULL,
source_name character varying(200) NOT NULL,
specification_name character varying(200) NOT NULL,
last_synchronized  timestamp with time zone,
synchronization_enabled boolean NOT NULL,
PRIMARY KEY (domain_name, source_schema, source_name)
);

CREATE OR REPLACE FUNCTION search.register_domain_source(
  p_domain_name character varying,
  p_source_type character varying,
  p_source_schema character varying,
  p_source_name character varying,
  p_specification_name character varying,
  p_synchronization_enabled boolean
) RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO search.domain_sources(
      domain_name,
      source_type,
      source_schema,
      source_name,
      specification_name,
      synchronization_enabled)
    VALUES
     (p_domain_name,
      p_source_type,
      p_source_schema,
      p_source_name,
      p_specification_name,
      p_synchronization_enabled);
  END; 
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION search.unregister_domain_source(
  p_domain_name character varying,
  p_source_schema character varying,
  p_source_name character varying
) RETURNS void AS
$BODY$
  BEGIN 
    DELETE FROM search.domain_sources
    WHERE domain_name = p_domain_name
      AND source_schema = p_source_schema
      AND source_name = p_source_name;
  END; 
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE VIEW search.active_sources AS SELECT 
  s.domain_name, 
  s.source_type, 
  s.source_schema, 
  s.source_name, 
  s.specification_name, 
  s.last_synchronized, 
  d.config_location 
FROM search.domain_sources AS s 
INNER JOIN search.domains d ON (s.domain_name=d.domain_name) 
WHERE s.synchronization_enabled and d.synchronization_enabled;

