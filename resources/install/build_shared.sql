
-- Create SHARED schema
-- ====================

CREATE SCHEMA shared;
COMMENT ON SCHEMA shared IS 'A schema to hold objects that will be shared across schemas';


CREATE TABLE shared.specification_registry
(name character varying (30) not null primary key,
 label  character varying (200) not null,
 vendor character varying (200) not null,
 version character varying (30),
 file_wildcard character varying(30),
 last_import timestamp with time zone,
 last_sent_to_editable timestamp with time zone
);

CREATE TABLE shared.specification_tables
(table_name character varying (200) not null primary key,
 specification_name character varying (30) not null);


CREATE OR REPLACE FUNCTION shared.register_specification(
  p_name character varying,
  p_label character varying,
  p_vendor character varying,
  p_version character varying,
  p_file_wildcard character varying
) RETURNS void AS
$BODY$
 BEGIN
   INSERT INTO shared.specification_registry(
     name,
     label,
     vendor,
     version,
     file_wildcard)
   VALUES (
     p_name,
     p_label,
     p_vendor,
     p_version,
     p_file_wildcard);   
 END;
$BODY$
LANGUAGE 'plpgsql';
 
CREATE OR REPLACE FUNCTION shared.unregister_specification(
  p_name character varying
) RETURNS void AS
$BODY$
 BEGIN
   DELETE FROM shared.specification_registry
   WHERE name = p_name;  
 END;
$BODY$
LANGUAGE 'plpgsql';

CREATE SEQUENCE shared.task_id;

CREATE OR REPLACE FUNCTION shared.get_task_id() RETURNS integer AS
$BODY$
 DECLARE
   r integer; 
 BEGIN
   SELECT nextval('shared.task_id')
   INTO r;
   RETURN r;
 END;
$BODY$
LANGUAGE 'plpgsql';

CREATE TYPE shared.chimp_message AS
   (level character varying(30),
    code character varying(30),    
    title character varying(200),
    affected_columns character varying(2000),
    affected_row_count integer,    
    content character varying(4000));

CREATE OR REPLACE FUNCTION shared.make_notice(p_code character varying, p_title character varying, p_affected_columns character varying, p_affected_row_count integer, p_content character varying) RETURNS shared.chimp_message AS
$BODY$
 DECLARE
   v_message shared.chimp_message; 
 BEGIN
   v_message.level = 'notice';
   v_message.code = p_code;    
   v_message.title = p_title;
   v_message.affected_columns = p_affected_columns;
   v_message.affected_row_count = p_affected_row_count;   
   v_message.content = p_content;   
   RETURN v_message;
 END;
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION shared.make_warning(p_code character varying, p_title character varying, p_affected_columns character varying, p_affected_row_count integer, p_content character varying) RETURNS shared.chimp_message  AS
$BODY$
 DECLARE
   v_message shared.chimp_message; 
 BEGIN
   v_message.level = 'warning';
   v_message.code = p_code;    
   v_message.title = p_title;
   v_message.affected_columns = p_affected_columns;
   v_message.affected_row_count = p_affected_row_count;   
   v_message.content = p_content;   
   RETURN v_message;
 END;
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION shared.make_error(p_code character varying, p_title character varying, p_affected_columns character varying, p_affected_row_count integer, p_content character varying) RETURNS shared.chimp_message AS
$BODY$
 DECLARE
   v_message shared.chimp_message; 
 BEGIN
   v_message.level = 'error';
   v_message.code = p_code;    
   v_message.title = p_title;
   v_message.affected_columns = p_affected_columns;
   v_message.affected_row_count = p_affected_row_count;   
   v_message.content = p_content;   
   RETURN v_message;
 END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION shared.make_exception(p_code character varying, p_title character varying, p_affected_columns character varying, p_affected_row_count integer, p_content character varying) RETURNS shared.chimp_message AS
$BODY$
 DECLARE
   v_message shared.chimp_message; 
 BEGIN
   v_message.level = 'exception';
   v_message.code = p_code;    
   v_message.title = p_title;
   v_message.affected_columns = p_affected_columns;
   v_message.affected_row_count = p_affected_row_count;   
   v_message.content = p_content;   
   RETURN v_message;
 END;
$BODY$
LANGUAGE 'plpgsql';

CREATE TABLE shared.current_tasks
  (task_id integer primary key,
   group_id character varying(200),
   stream character varying (200) not null,
   command character varying (30) not null,
   label_short character varying(200) not null,
   label_long character varying(2000),
   queued timestamp with time zone default statement_timestamp() NOT NULL,
   started timestamp with time zone,
   finished timestamp with time zone,
   state character varying(30) default 'pending' NOT NULL,
   process_id character varying (30),
   process_limit integer,
   scan_count integer,
   success_count integer,
   exception_count integer,
   error_count integer,
   warning_count integer,
   notice_count integer,
   ignored_count integer,
   audit_computer_name character varying(200),
   audit_operating_system character varying(200),
   audit_operating_system_release character varying(200),
   audit_username character varying(200),
   args character varying,
   worth_logging boolean not null,
   CHECK (state IN('pending','scanning','scanned', 'paused','finished','failed','processing')));
 
 CREATE INDEX queued_idx ON shared.current_tasks(queued);
  
CREATE OR REPLACE FUNCTION shared.queue_task
  (p_group_id character varying,
   p_stream character varying,
   p_command character varying,
   p_label_short character varying,
   p_label_long character varying,
   p_process_id character varying,
   p_process_limit integer,
   p_scan_count integer,
   p_audit_computer_name character varying,
   p_audit_operating_system character varying,
   p_audit_operating_system_release character varying,
   p_audit_username character varying,
   p_args character varying,
   p_worth_logging boolean) RETURNS integer AS
$BODY$
 DECLARE
   v_task_id integer; 
 BEGIN
   v_task_id = shared.get_task_id();

   INSERT INTO shared.current_tasks
    (task_id,
     group_id,
     stream,
     command,
     label_short,
     label_long,
     process_id,
     process_limit,
     scan_count,
     audit_computer_name,
     audit_operating_system,
     audit_operating_system_release,
     audit_username,
     args,
     worth_logging)
    VALUES
    (v_task_id,
     p_group_id,
     p_stream,
     p_command,
     p_label_short,
     p_label_long,
     p_process_id,
     p_process_limit,
     p_scan_count,
     p_audit_computer_name,
     p_audit_operating_system,
     p_audit_operating_system_release,
     p_audit_username,
     p_args,
     p_worth_logging);

   RETURN v_task_id;
 END;
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION shared.set_task_start
  (p_task_id integer,
   p_scan_required boolean,p_process_id character varying) RETURNS void AS
$BODY$
 BEGIN
   IF p_scan_required THEN
     UPDATE shared.current_tasks SET 
       started = statement_timestamp(),
       state = 'scanning',
       process_id = p_process_id
     WHERE task_id=p_task_id;
   ELSE
     UPDATE shared.current_tasks SET 
       started = statement_timestamp(),
       state = 'processing',
       process_id = p_process_id,
       success_count = 0,
       exception_count = 0,
       error_count = 0,
       warning_count = 0,
       notice_count = 0,
       ignored_count = 0
     WHERE task_id=p_task_id;
   END IF;
 END;
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION shared.set_scan_results
  (p_task_id integer,
   p_scan_count integer) RETURNS void AS
 $BODY$
 BEGIN  
   UPDATE shared.current_tasks SET 
     state = 'processing',
     scan_count=p_scan_count,
     success_count = 0,
     exception_count = 0,
     error_count = 0,
     warning_count = 0,
     notice_count = 0,
     ignored_count = 0
   WHERE task_id=p_task_id;
 END;
$BODY$
LANGUAGE 'plpgsql';
 
 
CREATE OR REPLACE FUNCTION shared.set_task_progress
  (p_task_id integer,
   p_latest_success_count integer,
   p_latest_exception_count integer,
   p_latest_error_count integer,
   p_latest_warning_count integer,
   p_latest_notice_count integer,
   p_latest_ignored_count integer
) RETURNS void AS
 $BODY$
 BEGIN  
   UPDATE shared.current_tasks SET 
      state = 'processing',
      success_count = p_latest_success_count,
      exception_count=p_latest_exception_count,
      error_count = p_latest_error_count,
      warning_count = p_latest_warning_count,
      notice_count = p_latest_notice_count,
      ignored_count = p_latest_ignored_count
   WHERE task_id=p_task_id;
 END;
$BODY$
LANGUAGE 'plpgsql';
 

CREATE TABLE shared.finished_tasks 
  (task_id integer primary key,
   group_id character varying(200),
   stream character varying (200) not null,
   command character varying (30) not null,
   label_short character varying(200) not null,
   label_long character varying(2000),
   queued timestamp with time zone NOT NULL,
   started timestamp with time zone NOT NULL,
   finished timestamp with time zone NOT NULL,
   state character varying(30) NOT NULL,
   process_limit integer,
   scan_count integer,
   success_count integer,
   exception_count integer,
   error_count integer,
   warning_count integer,
   notice_count integer,
   ignored_count integer,
   audit_computer_name character varying(200),
   audit_operating_system character varying(200),
   audit_operating_system_release character varying(200),
   audit_username character varying(200),
   args character varying);

CREATE INDEX started_idx ON shared.finished_tasks(started);



CREATE OR REPLACE FUNCTION shared.set_checkpoint_success(p_task_id integer, p_stream character varying)
  RETURNS void AS
$BODY$
 BEGIN

  UPDATE shared.current_tasks
  SET state='finished'
  WHERE task_id = p_task_id;
  
  INSERT INTO shared.finished_tasks
  (task_id,
   group_id,
   stream,
   command,
   label_short,
   label_long,
   queued,
   started,
   finished,
   state,
   process_limit,
   scan_count,
   success_count,
   exception_count,
   error_count,
   warning_count,
   notice_count,
   ignored_count,
   audit_computer_name,
   audit_operating_system,
   audit_operating_system_release,
   audit_username,
   args)
  SELECT 
  task_id,
  group_id,
  stream,
  command,
  label_short,
  label_long,
  queued,
  started,
  finished,
  state,
  process_limit,
  scan_count,
  success_count,
  exception_count,
  error_count,
  warning_count,
  notice_count,
  ignored_count,
  audit_computer_name,
  audit_operating_system,
  audit_operating_system_release,
  audit_username,
  args
  FROM shared.current_tasks
  WHERE stream=p_stream
  AND state='finished'
  AND worth_logging;

  DELETE 
  FROM shared.current_tasks
  WHERE stream=p_stream
  AND state='finished';
    
 END;
$BODY$
  LANGUAGE plpgsql;
  


CREATE OR REPLACE FUNCTION shared.set_checkpoint_failure(p_stream character varying)
  RETURNS void AS
$BODY$
 BEGIN 
  UPDATE shared.current_tasks
  SET state='pending'
  WHERE stream=p_stream
  AND state='finished';   
 END;
$BODY$
  LANGUAGE plpgsql;

  
CREATE OR REPLACE FUNCTION shared.set_task_finish
  (p_task_id integer,
   p_final_success_count integer,
   p_final_exception_count integer,
   p_final_error_count integer,
   p_final_warning_count integer,
   p_final_notice_count integer,
   p_final_ignored_count integer) RETURNS void AS
 $BODY$

 BEGIN  

     UPDATE shared.current_tasks SET 
       state='finished',
       finished=statement_timestamp(),
       success_count =  p_final_success_count,
       exception_count = p_final_exception_count,
       error_count = p_final_error_count,
       warning_count = p_final_warning_count,
       notice_count = p_final_notice_count,
       ignored_count = p_final_ignored_count
     WHERE task_id=p_task_id;

 END;
$BODY$
LANGUAGE 'plpgsql';


CREATE TABLE shared.task_messages
( id bigint primary key,
  task_id integer not null,
  table_name character varying(200),   
  seq integer not null,  
  level character varying(30),
  code character varying(30),    
  title character varying(200),
  affected_columns character varying(2000),
  affected_row_count integer,    
  content character varying(4000),
  CHECK (level IN('notice','warning','error','exception')));
 
 CREATE INDEX task_message_task_id ON shared.task_messages(task_id);
 CREATE INDEX task_message_seq ON shared.task_messages(task_id,seq);

CREATE SEQUENCE shared.task_message_id;

CREATE OR REPLACE FUNCTION shared.get_task_message_id() RETURNS bigint AS
$BODY$
 DECLARE
   r bigint; 
 BEGIN
   SELECT nextval('shared.task_message_id')
   INTO r;
   RETURN r;
 END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION shared.add_task_message
  (P_task_id integer,
   P_table_name character varying,   
   P_seq integer,  
   P_level character varying,
   P_code character varying,    
   P_title character varying,
   P_affected_columns character varying,
   P_affected_row_count integer,    
   p_content character varying) RETURNS bigint AS
 $BODY$
 DECLARE
   v_message_id bigint;
 BEGIN  
   v_message_id = shared.get_task_message_id();
   INSERT INTO shared.task_messages(
     id,
     task_id,
     table_name,   
     seq,  
     level,
     code,    
     title,
     affected_columns,
     affected_row_count,    
     content)
   VALUES (
     v_message_id,
     p_task_id,
     P_table_name,   
     P_seq,  
     P_level,
     P_code,    
     P_title,
     P_affected_columns,
     P_affected_row_count,    
     p_content);
     
   RETURN v_message_id;
 END;
$BODY$
LANGUAGE 'plpgsql';

-- ================
-- Dataitem objects
-- ================

CREATE TABLE shared.dataitems (
	name			CHARACTER VARYING(200) PRIMARY KEY,
	label			CHARACTER VARYING(200) NOT NULL,
	data_type CHARACTER VARYING(200) NOT NULL,
	size INTEGER,
	decimal_places INTEGER,
	description		CHARACTER VARYING(200),
	created			TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	last_modified	TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE shared.dataitem_tags (
	dataitem_name	CHARACTER VARYING(200) NOT NULL,
	tag				CHARACTER VARYING(200) NOT NULL,
	created			TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT dataitem_tags_fk
	FOREIGN KEY (dataitem_name) REFERENCES shared.dataitems(name)
);

CREATE UNIQUE INDEX dataitem_tags_idx ON shared.dataitem_tags(dataitem_name, tag);

CREATE TABLE shared.dataitem_provenance (
	dataitem_name	CHARACTER VARYING(200) NOT NULL,
	specification	CHARACTER VARYING(200) NOT NULL,
	table_name CHARACTER VARYING(200) NOT NULL,
	column_name CHARACTER VARYING(200) NOT NULL,
	editable BOOLEAN NOT NULL,	
	CONSTRAINT dataitem_provenance_fk
	FOREIGN KEY (dataitem_name) REFERENCES shared.dataitems(name)
);

CREATE INDEX dataitem_provenance_idx ON shared.dataitem_provenance(dataitem_name);
CREATE INDEX dataitem_provenance_spec_idx ON shared.dataitem_provenance(specification);

CREATE TABLE shared.dataitem_tag_provenance (
	dataitem_name	CHARACTER VARYING(200) NOT NULL,
	tag				CHARACTER VARYING(200) NOT NULL,
	specification	CHARACTER VARYING(200) NOT NULL,	
	CONSTRAINT dataitem_tag_provenance_fk
	FOREIGN KEY (dataitem_name,tag) REFERENCES shared.dataitem_tags(dataitem_name,tag)
);

CREATE INDEX dataitem_tag_provenance_idx ON shared.dataitem_tag_provenance(dataitem_name, tag);
CREATE INDEX dataitem_tag_provenance_spec_idx ON shared.dataitem_tag_provenance(specification);
CREATE INDEX dataitem_tag_provenance_all_idx ON shared.dataitem_tag_provenance(dataitem_name, tag, specification);


CREATE OR REPLACE FUNCTION shared.register_dataitem(
	p_specification CHARACTER VARYING,
	p_name CHARACTER VARYING,
	p_table_name CHARACTER VARYING,
	p_column_name CHARACTER VARYING,
	p_editable BOOLEAN,
	p_label CHARACTER VARYING,
	p_data_type CHARACTER VARYING,
	p_size INTEGER,
	p_decimal_places INTEGER,	
	p_description CHARACTER VARYING,
	p_tags CHARACTER VARYING)
RETURNS VOID AS
$BODY$
DECLARE
	v_spec CHARACTER VARYING;
	v_name CHARACTER VARYING;
	v_table_name CHARACTER VARYING;
	v_column_name CHARACTER VARYING;
	v_tags CHARACTER VARYING;
	v_tag_part CHARACTER VARYING;
	v_my_rec RECORD;
	v_tag_array CHARACTER VARYING ARRAY;
BEGIN

-- force all key fields to lower case
	v_spec := lower(p_specification);
	v_name := lower(p_name);
	v_table_name := lower(p_table_name);
	v_column_name := lower(p_column_name);

	IF p_tags IS NULL THEN
		v_tags := NULL;
	ELSE
		v_tags := lower(trim(tags));
		IF length(v_tags) = 0 THEN
			v_tags := NULL;
		END IF;
	END IF;
	
-- does item exist in dataitems?
	SELECT * 
	INTO v_my_rec 
	FROM shared.dataitems d 
	WHERE d.name = v_name;
	
-- if no then create one
	IF NOT FOUND THEN
		INSERT INTO shared.dataitems (
		  name, 
		  label, 
      data_type,
	    size,
	    decimal_places,
		  description) 
		VALUES (
		  v_name, 
		  p_label,
      p_data_type,
	    p_size,
	    p_decimal_places,		   
		  p_description);
	END IF;

-- create spec entry in shared.dataitem_provenance
-- does item exist in dataitems_provenance?
	SELECT * 
	INTO v_my_rec
	FROM shared.dataitem_provenance p
	WHERE p.dataitem_name = v_name
	AND p.specification = v_spec;

-- if no then create one
	IF NOT FOUND THEN
		INSERT INTO shared.dataitem_provenance (
		  dataitem_name,
		  specification,
		  table_name,
		  column_name,
		  editable)
		VALUES (
		  v_name, 
		  v_spec,
		  v_table_name,
		  v_column_name,
		  p_editable);
	END IF;

-- proccess any tags
	IF v_tags IS NULL THEN
	-- do nothing
		NULL;
	ELSE
		v_tag_array = string_to_array(v_tags, ',');
		
		FOR i IN 1..array_length(v_tag_array, 1) LOOP
			v_tag_part := trim(trim(trim(v_tag_array[i]),''''));
--			see if there's a tag row for this item already
			SELECT * INTO v_my_rec FROM shared.dataitem_tags t WHERE t.dataitem_name = v_name AND t.tag = v_tag_part;
--			if not the add it
			IF NOT FOUND THEN
				INSERT INTO shared.dataitem_tags (dataitem_name, tag) VALUES (v_name, v_tag_part);
			END IF;

--			see if there's a tag provenance row for this item already
			SELECT * 
			INTO v_my_rec 
			FROM shared.dataitem_tag_provenance tp 
			WHERE tp.dataitem_name = v_name
			AND tp.tag = v_tag_part
			AND tp.specification = v_spec;
			
--			if not the add it
			IF NOT FOUND THEN
				INSERT INTO shared.dataitem_tag_provenance (dataitem_name, tag, specification) VALUES (v_name, v_tag_part, v_spec);
			END IF;

		END LOOP;
	END IF;

END
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION shared.unregister_dataitems(
	specification CHARACTER VARYING)
	RETURNS VOID AS
$BODY$
DECLARE
	p_spec CHARACTER VARYING;
	my_rec RECORD;
BEGIN
	IF specification IS NULL THEN
		RAISE EXCEPTION 'Specification must not be NULL';
	END IF;
	
	p_spec = trim(lower(specification));
	
-- DELETE FROM shared.dataitem_provenance
	DELETE FROM shared.dataitem_provenance p WHERE p.specification = p_spec;
-- DELETE FROM shared.dataitem_tag_provenance
	DELETE FROM shared.dataitem_tag_provenance tp WHERE tp.specification = p_spec;
	
-- look for any rows in shared.dataitem_tags which don't have at least one
-- spec record in shared.dataitem_tag_provenace and delete them
	FOR my_rec IN SELECT * FROM (
		SELECT t.dataitem_name AS dataitem_name, t.tag AS tag, p.tag AS subtag
			FROM shared.dataitem_tags t LEFT OUTER JOIN shared.dataitem_tag_provenance p
				on t.dataitem_name = p.dataitem_name AND t.tag = p.tag) a 
		WHERE a.subtag IS NULL LOOP
		DELETE FROM shared.dataitem_tags WHERE dataitem_name = my_rec.dataitem_name AND tag = my_rec.tag;
	END LOOP;
	
-- look for any rows in shared.dataitems which don't have any child records in either
-- shared.dataitem_tags or shared.dataitem_provenance and delete them
	FOR my_rec IN SELECT * FROM (
		SELECT d.name AS name, t.tag AS tag, s.specification as specification 
			FROM shared.dataitems d  LEFT OUTER JOIN shared.dataitem_tags t
			ON d.name = t.dataitem_name
			LEFT OUTER JOIN shared.dataitem_provenance s
			ON d.name = s.dataitem_name
		) a WHERE a.tag IS NULL AND a.specification IS NULL LOOP
		DELETE FROM shared.dataitems WHERE name = my_rec.name;
	END LOOP;
	
END

$BODY$
LANGUAGE 'plpgsql';



-- ==================
-- Utility functions:
-- ===================

CREATE OR REPLACE FUNCTION shared.convert_Coordinate_to_lat_lon(p_x numeric, p_y numeric, p_srid integer) RETURNS character varying AS 
$BODY$
  DECLARE
    v_lat_lon geometry;
    v_pretty character varying(200);
  BEGIN
    SELECT ST_TRANSFORM(ST_GeomFromText('POINT('||p_x||' '||p_y||')',p_srid), 4326)
    INTO v_lat_lon;
    v_pretty=ST_Y(v_lat_lon)||','||ST_X(v_lat_lon);
    RETURN v_pretty;
  END;
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION shared.different(value1 anyelement, value2 anyelement)
  RETURNS boolean AS
$BODY$
 DECLARE
   is_different BOOLEAN = FALSE; 
 BEGIN
   IF (value1 is null and value2 is not null) OR
      (value1 is not null and value2 is null) OR
     ((value1 is not null and value2 is not null) and value1 != value2) THEN
      is_different=TRUE;
   END IF;
   RETURN is_different;
 END;
$BODY$
LANGUAGE 'plpgsql';
COMMENT ON FUNCTION shared.different(value1 anyelement, value2 anyelement) IS 'Function that returns true if the two supplied values are different, else false. This comparison is ''null safe.''';

CREATE FUNCTION shared.instr(varchar, varchar) RETURNS integer AS $$
DECLARE
    pos integer;
BEGIN
    pos:= shared.instr($1, $2, 1);
    RETURN pos;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;


CREATE FUNCTION shared.instr(string varchar, string_to_search varchar, beg_index integer)
RETURNS integer AS $$
DECLARE
    pos integer NOT NULL DEFAULT 0;
    temp_str varchar;
    beg integer;
    length integer;
    ss_length integer;
BEGIN
    IF beg_index > 0 THEN
        temp_str := substring(string FROM beg_index);
        pos := position(string_to_search IN temp_str);

        IF pos = 0 THEN
            RETURN 0;
        ELSE
            RETURN pos + beg_index - 1;
        END IF;
    ELSE
        ss_length := char_length(string_to_search);
        length := char_length(string);
        beg := length + beg_index - ss_length + 2;

        WHILE beg > 0 LOOP
            temp_str := substring(string FROM beg FOR ss_length);
            pos := position(string_to_search IN temp_str);

            IF pos > 0 THEN
                RETURN beg;
            END IF;

            beg := beg - 1;
        END LOOP;

        RETURN 0;
    END IF;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;


CREATE FUNCTION shared.instr(string varchar, string_to_search varchar,
                      beg_index integer, occur_index integer)
RETURNS integer AS $$
DECLARE
    pos integer NOT NULL DEFAULT 0;
    occur_number integer NOT NULL DEFAULT 0;
    temp_str varchar;
    beg integer;
    i integer;
    length integer;
    ss_length integer;
BEGIN
    IF beg_index > 0 THEN
        beg := beg_index;
        temp_str := substring(string FROM beg_index);

        FOR i IN 1..occur_index LOOP
            pos := position(string_to_search IN temp_str);

            IF i = 1 THEN
                beg := beg + pos - 1;
            ELSE
                beg := beg + pos;
            END IF;

            temp_str := substring(string FROM beg + 1);
        END LOOP;

        IF pos = 0 THEN
            RETURN 0;
        ELSE
            RETURN beg;
        END IF;
    ELSE
        ss_length := char_length(string_to_search);
        length := char_length(string);
        beg := length + beg_index - ss_length + 2;

        WHILE beg > 0 LOOP
            temp_str := substring(string FROM beg FOR ss_length);
            pos := position(string_to_search IN temp_str);

            IF pos > 0 THEN
                occur_number := occur_number + 1;

                IF occur_number = occur_index THEN
                    RETURN beg;
                END IF;
            END IF;

            beg := beg - 1;
        END LOOP;

        RETURN 0;
    END IF;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;


CREATE OR REPLACE FUNCTION shared.normalize_postcode(raw_postcode character varying)
RETURNS character varying AS $$  
DECLARE
  r character varying(30);
  split_pos integer;
BEGIN
  IF raw_postcode IS NOT NULL THEN
    r := REPLACE(raw_postcode,' ','');
    IF length(r)>3 THEN
      split_pos := length(r)-3;
      r:= SUBSTR(r,1,split_pos)||' '||SUBSTR(r,split_pos+1);
    END IF;
  END IF;
  RETURN r;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;


CREATE OR REPLACE FUNCTION shared.prefix_string(raw_text character varying, prefix character varying)
RETURNS character varying AS $$  
DECLARE
  r character varying(500);
BEGIN
  r = prefix||raw_text;
  RETURN r;
END;
$$ LANGUAGE plpgsql STRICT IMMUTABLE;

CREATE OR REPLACE FUNCTION shared.is_integer(p_text character varying) RETURNS BOOLEAN AS $$
DECLARE
  r BOOLEAN = FALSE;
BEGIN
  IF p_text IS NOT NULL THEN
    r = p_text ~ E'^\\s*(-)?[0-9]+\\s*$';
  END IF;
  RETURN r;
END;
$$
LANGUAGE 'plpgsql' VOLATILE;


CREATE OR REPLACE FUNCTION shared.simple_text(p_text character varying)
  RETURNS character varying AS
$BODY$
DECLARE
  r character varying(500);
BEGIN
  IF p_text IS NOT NULL THEN
    r = translate(lower(trim(p_text)),  '|,.!"$*()_+-={}[]:;@''', 
                                         '                     ');
    WHILE position('  ' IN r)>0 LOOP
      r = REPLACE(r, '  ', ' ');
    END LOOP;
  END IF;
  RETURN r;
END;
$BODY$
  LANGUAGE 'plpgsql' IMMUTABLE STRICT
  COST 100;

  
CREATE OR REPLACE FUNCTION shared.get_distance(p_x1 numeric, p_y1 numeric, p_x2 numeric, p_y2 numeric) RETURNS NUMERIC AS $$
DECLARE
  w numeric;
  h numeric;
BEGIN
  w = p_x1 - p_x2;
  h = p_y1 - p_y2;
  RETURN |/(w*w+h*h);
END;
$$ LANGUAGE plpgsql;
  

CREATE OR REPLACE FUNCTION shared.get_distance(p_x1 float, p_y1 float, p_x2 float, p_y2 float) RETURNS NUMERIC AS $$
DECLARE
  w numeric;
  h numeric;
BEGIN
  w = p_x1 - p_x2;
  h = p_y1 - p_y2;
  RETURN |/(w*w+h*h);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION shared.get_average_distance(p_x numeric, p_y numeric, p_table_name character varying, p_column character varying, p_average_count integer, p_srid integer) RETURNS integer AS
$BODY$
DECLARE
  v_avg_distance integer;
  v_sql character varying; 
BEGIN
  v_sql = 'SELECT floor(avg(distance)) avg_distance '||
  'FROM (SELECT floor(ST_Distance(ST_GeomFromText(''POINT('||p_x::character varying||' '||p_y::character varying||')'','||p_srid::character varying||'), '||p_column||')) as distance '||
  'FROM '||p_table_name||' order by 1 asc limit '||p_average_count::character varying||') as top_3;';
  EXECUTE v_sql
  INTO v_avg_distance;
  RETURN v_avg_distance;
END;
$BODY$
LANGUAGE plpgsql;
  
CREATE OR REPLACE FUNCTION shared.make_sql_statement(p_select_clause character varying, p_from_clause character varying, p_filter_restrictions character varying[], p_order_by_clause character varying) RETURNS CHARACTER VARYING AS $$
DECLARE
  v_sql character varying(2000);
  v_conditions character varying(2000);
BEGIN

  IF p_select_clause IS NOT NULL AND p_from_clause IS NOT NULL THEN
    v_sql = 'SELECT '||p_select_clause||' FROM '||p_from_clause;

    v_conditions = ARRAY_TO_STRING (p_filter_restrictions,' AND ');
        
    IF v_conditions IS NOT NULL THEN
      v_sql = v_sql ||' WHERE '|| v_conditions;
    END IF;

    IF p_order_by_clause IS NOT NULL THEN
      v_sql=v_sql||' ORDER BY '||p_order_by_clause;
    END IF;
    
  END IF;
  RETURN v_sql;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION shared.explode_string_to_select_clause(p_string character varying, p_record_prefixes character varying, p_variables character varying)
  RETURNS character varying AS
$BODY$
DECLARE
  r character varying;
  v_variables character varying[];
  v_record_prefixes character varying [];
  v_pos integer;
  v_overlay_value character varying;
  v_variable_index integer = 1;
  v_variable_parts character varying [];
BEGIN
  v_variables = string_to_array(p_variables, ',');
  v_record_prefixes = string_to_array(p_record_prefixes, ',');
  r = ''''||REPLACE(p_string,'''','''''')||'''';
  FOR i IN 1..shared.count_in_string(p_string,'%') LOOP
    v_pos = shared.instr(r, '%', 1, 1);

    v_overlay_value = '''||';
    v_variable_parts = string_to_array(v_variables[v_variable_index], '.');
    IF array_upper(v_variable_parts,1)=2 THEN
      FOR j IN 1..array_upper(v_variable_parts,1) LOOP
        IF (v_record_prefixes[j]=v_variable_parts[1] OR v_record_prefixes[j]='p_'||v_variable_parts[1]) THEN
          v_overlay_value = v_overlay_value || '$'||j::character varying||'.'||v_variable_parts[2]||'::character varying';
        END IF;
      END LOOP;
    ELSE
	    v_overlay_value = v_overlay_value || v_variables[v_variable_index]||'::character varying';
    END IF;
       
    v_overlay_value = v_overlay_value || '||''';
        
    r = overlay(r placing v_overlay_value from v_pos for 1);
    v_variable_index = v_variable_index + 1;
  END LOOP;
  RETURN r;
END;
$BODY$
  LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION shared.batch_cutter(p_target_table character varying, p_pk_column character varying, p_order_by character varying, p_batch_column character varying, p_max_batch_id integer)
  RETURNS void AS
$BODY$
  DECLARE
    v_sql character varying;
    v_count integer;
    v_record record;
    v_update character varying;
    v_batch int;
    v_batch_size numeric;
    i integer;
    
  BEGIN

    -- Function to chunk a table into numbered batches... 
    -- for the purposes of splitting work into groups for manual data processing/reviewing apps.
    
    v_sql := 'SELECT count(*) FROM '||p_target_table;
    EXECUTE v_sql
    INTO v_count
    USING p_target_table;
    RAISE NOTICE 'ROW COUNT: %', v_count;


    v_batch_size = CEIL(v_count::numeric/p_max_batch_id::numeric);
    RAISE NOTICE 'BATCH SIZE: %', v_batch_size;

    v_sql := 'SELECT DISTINCT '||p_pk_column||' AS pk FROM '||p_target_table||' ORDER BY '||p_order_by;
    v_update := 'UPDATE ' || p_target_table || ' SET '||p_batch_column||'=$1 WHERE ' ||p_pk_column|| '=$2';
    v_batch = 1;

    i = 0;
    FOR v_record IN EXECUTE v_sql LOOP
      RAISE NOTICE '%', v_record.pk;

      EXECUTE v_update
      USING v_batch, v_record.pk;

      i = i + 1;
      IF i = v_batch_size THEN
        i = 0;
        IF v_batch < p_max_batch_id THEN
          v_batch := v_batch + 1;
        END IF;
      END IF;
        
      
    END LOOP;
    
  END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-- 
-- $Id: cleanGeometry.sql 2008-04-24 10:30Z Dr. Horst Duester $
--
-- cleanGeometry - remove self- and ring-selfintersections from 
--                 input Polygon geometries 
-- http://www.kappasys.ch
-- Copyright 2008 Dr. Horst Duester
-- Version 1.0
-- contact: horst dot duester at kappasys dot ch
--
-- This is free software; you can redistribute and/or modify it under
-- the terms of the GNU General Public Licence. See the COPYING file.
-- This software is without any warrenty and you use it at your own risk
--  
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

CREATE OR REPLACE FUNCTION cleanGeometry(geometry)
  RETURNS geometry AS
$BODY$
DECLARE
  inGeom ALIAS for $1;
  outGeom geometry;
  tmpLinestring geometry;

Begin
  
  outGeom := NULL;
  
-- Clean Process for Polygon 
  IF (GeometryType(inGeom) = 'POLYGON' OR GeometryType(inGeom) = 'MULTIPOLYGON') THEN

-- Only process if geometry is not valid, 
-- otherwise put out without change
    if not isValid(inGeom) THEN
    
-- create nodes at all self-intersecting lines by union the polygon boundaries
-- with the startingpoint of the boundary.  
      tmpLinestring := st_union(st_multi(st_boundary(inGeom)),st_pointn(boundary(inGeom),1));
      outGeom = buildarea(tmpLinestring);      
      IF (GeometryType(inGeom) = 'MULTIPOLYGON') THEN      
        RETURN st_multi(outGeom);
      ELSE
        RETURN outGeom;
      END IF;
    else    
      RETURN inGeom;
    END IF;


------------------------------------------------------------------------------
-- Clean Process for LINESTRINGS, self-intersecting parts of linestrings 
-- will be divided into multiparts of the mentioned linestring 
------------------------------------------------------------------------------
  ELSIF (GeometryType(inGeom) = 'LINESTRING') THEN
    
-- create nodes at all self-intersecting lines by union the linestrings
-- with the startingpoint of the linestring.  
    outGeom := st_union(st_multi(inGeom),st_pointn(inGeom,1));
    RETURN outGeom;
  ELSIF (GeometryType(inGeom) = 'MULTILINESTRING') THEN 
    outGeom := multi(st_union(st_multi(inGeom),st_pointn(inGeom,1)));
    RETURN outGeom;
  ELSE 
    RAISE NOTICE 'The input type % is not supported',GeometryType(inGeom);
    RETURN inGeom;
  END IF;	  
End;$BODY$
  LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION shared.count_in_string(text, text) 
RETURNS integer AS $$
  SELECT Array_upper(String_to_array($1,$2),1) - 1;
$$ LANGUAGE SQL IMMUTABLE;
