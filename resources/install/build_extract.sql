CREATE SCHEMA extract;
COMMENT ON SCHEMA extract IS 'A schema to store objects concerned with extracting data from out of Chimp';

CREATE SEQUENCE extract.extract_seq START WITH 10;

CREATE TABLE extract.extract_format_registry
(specification_name character varying (30) not null,
 extract_format character varying (30) not null,
 extractor_type character varying (30) not null,
 last_extract timestamp with time zone,
 PRIMARY KEY (specification_name, extract_format)
);

CREATE TABLE extract.extract_history
(extract_id PRIMARY KEY default nextval('extract.extract_seq'),
 specification_name character varying (30) not null,
 extract_format character varying (30) not null,
 extract_timestamp timestamp with time zone,
 content_report character varying (4000)
);


CREATE OR REPLACE FUNCTION extract.register_extract_format(
  p_specification_name character varying,
  p_format character varying,
  p_extractor_type character varying
) RETURNS void AS
$BODY$
 BEGIN
   INSERT INTO extract.extract_format_registry (
     specification_name,
     extract_format,
     extractor_type
     )
   VALUES (
     p_specification_name,
     p_format,
     p_extractor_type);   
 END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION extract.unregister_extract_format(
  p_specification_name character varying,
  p_format character varying
) RETURNS void AS
$BODY$
 BEGIN
   DELETE FROM extract.extract_format_registry
   WHERE specification_name = p_specification_name
   AND extract_format = p_format;
 END;
$BODY$
LANGUAGE 'plpgsql';
