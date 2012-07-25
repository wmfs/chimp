-- Create STORE schema
-- =====================

CREATE SCHEMA store;
COMMENT ON SCHEMA store IS 'A schema to store things.';

CREATE SEQUENCE store.imports_seq
  INCREMENT 1
  MINVALUE 1
  START 10
  CACHE 1;

CREATE SEQUENCE store.import_sets_seq
  INCREMENT 1
  MINVALUE 1
  START 10
  CACHE 1;

CREATE SEQUENCE store.thread_seq
  INCREMENT 1
  MINVALUE 1
  START 10
  CACHE 1;
    
CREATE TABLE store.imports
(
  id integer NOT NULL,
  source character varying(30) NOT NULL,
  specification character varying(30) NOT NULL,
  thread_id integer,
  state character varying(30) NOT NULL,  
  info  character varying(200),   
  import_start timestamp with time zone default statement_timestamp() NOT NULL,
  import_end timestamp with time zone,  
  audit_computer_name character varying(200),
  audit_operating_system character varying(200),
  audit_operating_system_release character varying(200), 
  audit_username character varying(200),
  implied_working_count integer default 0 NOT NULL,
  implied_held_count integer default 0 NOT NULL,
  implied_exception_count integer default 0 NOT NULL,
  comments character varying(4000),
  CONSTRAINT import_id PRIMARY KEY (id)
);
COMMENT ON TABLE store.imports IS 'Information about each import';


CREATE INDEX import_state ON store.imports(state);
CREATE INDEX import_specification ON store.imports(specification);
CREATE INDEX import_start ON store.imports(import_start);

CREATE TABLE store.import_sets
(
  id integer NOT NULL,
  import_id integer NOT NULL,
  set_seq integer NOT NULL,
  short_label character varying(200) NOT NULL,
  full_label character varying(200) NOT NULL,
  state character varying(30) NOT NULL,  
  info  character varying(200),  
  import_mode  character varying(30) DEFAULT 'pending'::character varying,  
  import_start timestamp with time zone default statement_timestamp() NOT NULL,
  import_end timestamp with time zone,  
  scan_count integer default 0 NOT NULL,
  working_count integer default 0 NOT NULL,
  held_count integer default 0 NOT NULL,
  exception_count integer default 0 NOT NULL,
  unsupported_count integer default 0 NOT NULL,
  line_limit integer,
  commit_mode character varying(30) NOT NULL,   
  hold_mode character varying(30) NOT NULL,  
  comments character varying(4000),
  committed boolean NOT NULL DEFAULT false,
  CONSTRAINT import_set_id PRIMARY KEY (id)
);
COMMENT ON TABLE store.import_sets IS 'Each import consists of 1 or more sets which contain incoming data';
CREATE INDEX import_sets_set_seq ON store.import_sets(import_id, set_seq);

ALTER TABLE store.import_sets
  ADD CONSTRAINT import_sets_fkey FOREIGN KEY (import_id)
      REFERENCES store.imports (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION;
	  		 	  
CREATE OR REPLACE FUNCTION store.get_new_thread_id()
  RETURNS integer AS
$BODY$
  DECLARE
    v_thread_id integer;
  BEGIN
    SELECT nextval('store.thread_seq')
	INTO v_thread_id;
    RETURN(v_thread_id);
  END;
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION store.get_new_import_id()
  RETURNS integer AS
$BODY$
  DECLARE
    v_import_id integer;
  BEGIN
    SELECT nextval('store.imports_seq')
	INTO v_import_id;
    RETURN(v_import_id);
  END;
$BODY$
LANGUAGE 'plpgsql';
	  
CREATE OR REPLACE FUNCTION store.get_new_import_set_id()
  RETURNS integer AS
$BODY$
  DECLARE
    v_import_set_id integer;
  BEGIN
    SELECT nextval('store.import_sets_seq')
	INTO v_import_set_id;
    RETURN(v_import_set_id);
  END;
$BODY$
LANGUAGE 'plpgsql';
	  	
CREATE OR REPLACE FUNCTION store.register_new_import(p_source character varying, p_specification character varying, p_audit_computer_name character varying, p_audit_operating_system character varying, p_audit_operating_system_release character varying, p_audit_username character varying, p_comments character varying)
  RETURNS integer AS
$BODY$
  DECLARE
    v_import_id integer;
  BEGIN  
    v_import_id= store.get_new_import_id();
    INSERT INTO store.imports(id,
                               source,
                               specification,
	                             state,  
                               audit_computer_name,
                               audit_operating_system,
                               audit_operating_system_release, 
                               audit_username,
                               comments)
    VALUES (v_import_id,
	        p_source,
            p_specification,
            'running',
            p_audit_computer_name,
            p_audit_operating_system,
			      p_audit_operating_system_release,
            p_audit_username,
            p_comments);
	                                                                     
    RETURN(v_import_id);    
  END; 
$BODY$
  LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION store.register_new_import_set(p_import_id integer,
                                                               p_set_seq integer,
                                                               p_full_label character varying,
															                                 p_short_label character varying,
															                                 p_line_limit integer,
															                                 p_commit_mode character varying,
															                                 p_hold_mode character varying,
                                                               p_comments character varying) RETURNS integer AS
$BODY$
  DECLARE
    v_import_set_id integer;
  BEGIN
    v_import_set_id=store.get_new_import_set_id();
    INSERT INTO store.import_sets(id,
                                   import_id,
                                   set_seq,
				                           full_label,
				                           short_label,
                                   state,  
                                   line_limit,
                                   commit_mode,   
                                   hold_mode,  
                                   comments)
    VALUES (v_import_set_id,
	           p_import_id,
             p_set_seq,
	       		 p_full_label,
			       p_short_label,
            'scanpending',
             p_line_limit,
             p_commit_mode,   
             p_hold_mode,  
             p_comments);
	                                                                     
    RETURN(v_import_set_id);    
  END; 
$BODY$
LANGUAGE 'plpgsql';



CREATE OR REPLACE FUNCTION store.register_start_of_scan(p_import_set_id integer, p_info character varying) RETURNS void AS
$BODY$
  BEGIN 
    UPDATE store.import_sets
	SET state='scanning',
		info=p_info
	WHERE id = p_import_set_id;

	UPDATE store.imports
	SET state='scanning'
    WHERE id=(SELECT import_id FROM store.import_sets WHERE id=p_import_set_id);		
  END; 
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION store.register_scan_finished(p_import_set_id integer, p_import_mode character varying, p_scan_count integer) RETURNS void AS
$BODY$
  BEGIN 
    update store.import_sets
    set import_mode = p_import_mode,
    scan_count=p_scan_count,
    state='queued'
    where id = p_import_set_id;
    
    update store.imports
    set state='queued'
    WHERE id=(SELECT import_id FROM store.import_sets WHERE id=p_import_set_id);		
    
  END; 
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION store.register_start_of_set_import(p_import_set_id integer, p_info character varying) RETURNS void AS
$BODY$
  BEGIN 
    UPDATE store.import_sets
	SET state='loading',
		info=p_info,
		import_start=statement_timestamp()
	WHERE id = p_import_set_id;

	UPDATE store.imports
	SET state='loading'
    WHERE id=(SELECT import_id FROM store.import_sets WHERE id=p_import_set_id);		
  END; 
$BODY$
LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION store.register_import_set_finish(p_import_set_id integer, p_working_count integer, p_held_count integer, p_exception_count integer, p_unsupported_count integer) RETURNS void AS
$BODY$
  BEGIN 
    UPDATE store.import_sets
	SET state='loaded',
		import_end=statement_timestamp(),
    working_count=p_working_count,
    held_count =p_held_count,
    exception_count =p_exception_count,
    unsupported_count =p_unsupported_count
	WHERE id = p_import_set_id;
	
  END; 
$BODY$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION store.register_import_finished(p_import_id integer, p_info character varying) RETURNS void AS
$BODY$
  BEGIN  
    UPDATE store.imports
    SET state='loaded',
	info=p_info,
	import_end=statement_timestamp()
	WHERE id=p_import_id;
  END; 
$BODY$
LANGUAGE 'plpgsql';
  
CREATE OR REPLACE FUNCTION store.register_import_progress(p_import_set_id integer, p_working_count integer, p_held_count integer, p_exception_count integer, p_unsupported_count integer) RETURNS void AS
$BODY$
  BEGIN 
    UPDATE store.import_sets
	SET working_count=p_working_count,
    held_count =p_held_count,
    exception_count =p_exception_count,
    unsupported_count=p_unsupported_count
	WHERE id = p_import_set_id;
  END; 
$BODY$
LANGUAGE 'plpgsql';

  
CREATE OR REPLACE FUNCTION store.import_set_percentage_complete(p_scan_count integer, p_working_count integer, p_held_count integer, p_exception_count integer, p_unsupported_count integer)
  RETURNS float AS
$BODY$
  DECLARE
    v_percentage float;
	v_processed_count float;
  BEGIN 
	
	  IF p_scan_count>0 THEN
	    v_processed_count = p_working_count+p_held_count+p_exception_count+p_unsupported_count;
	    v_percentage = (v_processed_count::float / p_scan_count::float) * 100;
		v_percentage = round(v_percentage::numeric,4);
	  END IF;
	
	RETURN(v_percentage);
  END; 
$BODY$
  LANGUAGE 'plpgsql';

CREATE TABLE store.import_sequencer(
import_id integer not null,
table_name character varying(200) not null,
seq integer not null,
record_state character varying(10) not null,
action character varying(10) not null);

CREATE INDEX store_import_sequencer_import_id ON store.import_sequencer(import_id);
CREATE INDEX store_import_sequencer_seq ON store.import_sequencer(seq);

CREATE OR REPLACE FUNCTION store.add_to_sequencer(p_import_id integer, p_table_name character varying, p_seq integer, p_record_state character varying, p_action character varying)
  RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO store.import_sequencer(
      import_id,
      table_name,
      seq,
      record_state,
      action)
    VALUES
     (p_import_id, 
      p_table_name, 
      p_seq, 
      p_record_state,
      p_action);
  END; 
$BODY$
  LANGUAGE 'plpgsql';

CREATE TABLE store.import_set_sequencer(
import_id integer not null,
import_set_id integer not null,
table_name character varying(200) not null,
line_no integer not null,
record_state character varying(10) not null,
action character varying(10));

CREATE INDEX store_import_set_sequencer_import_id ON store.import_set_sequencer(import_id);
CREATE INDEX store_import_set_sequencer_import_set_id ON store.import_set_sequencer(import_set_id);
CREATE INDEX store_import_set_sequencer_line_no ON store.import_set_sequencer(line_no);

CREATE OR REPLACE FUNCTION store.add_to_set_sequencer(p_import_id integer, p_import_set_id integer, p_table_name character varying, p_line_no integer, p_record_state character varying, p_action character varying)
  RETURNS void AS
$BODY$
  BEGIN 
    INSERT INTO store.import_set_sequencer(
      import_id,
      import_set_id,
      table_name,
      line_no,
      record_state,
      action)
    VALUES
     (p_import_id, 
      p_import_set_id,
      p_table_name, 
      p_line_no, 
      p_record_state,
      p_action);
  END; 
$BODY$
  LANGUAGE 'plpgsql';


CREATE TABLE store.import_messages(
import_id integer not null,
table_name character varying(200),
affected_columns character varying(200),
seq integer not null,
record_id bigint,
domain character varying(30),
message_no integer,
message_level integer,
message character varying(4000),
detail character varying(4000));

CREATE INDEX import_messages_import_id ON store.import_messages(import_id, seq);
CREATE INDEX import_messages_quick_find ON store.import_messages(table_name, record_id);

CREATE OR REPLACE FUNCTION store.add_import_message(p_import_id integer, p_table_name character varying, p_affected_columns character varying, p_record_id bigint, p_seq integer, p_domain character varying, p_message_no integer, p_message_level integer, p_message character varying, p_detail character varying)
  RETURNS void AS
$BODY$
  BEGIN
    INSERT INTO store.import_messages
    (import_id,
     table_name,
     affected_columns,
     seq,
     record_id,
     domain,
     message_no,
     message_level,
     message,
     detail)
   VALUES
    (p_import_id,
     p_table_name,
     p_affected_columns,
     p_seq,
     p_record_id,
     p_domain,
     p_message_no,
     p_message_level,
     p_message,
     p_detail);
  END;
$BODY$
  LANGUAGE 'plpgsql';
 
CREATE TABLE store.import_set_messages(
import_id integer not null,
import_set_id integer not null,
table_name character varying(200),
affected_columns character varying(200),
record_id bigint,
line_no integer,
domain character varying(30),
message_no integer,
message_level integer,
message character varying(4000),
detail character varying(4000));

CREATE INDEX import_set_messages_import_id ON store.import_set_messages(import_id);
CREATE INDEX import_set_messages_import_set_id ON store.import_set_messages(import_set_id);
CREATE INDEX import_set_messages_quick_find ON store.import_set_messages(table_name, record_id);

CREATE OR REPLACE FUNCTION store.add_import_set_message(p_import_id integer, p_import_set_id integer, p_table_name character varying, p_affected_columns character varying, p_record_id bigint, p_line_no integer, p_domain character varying, p_message_no integer, p_message_level integer, p_message character varying, p_detail character varying)
  RETURNS void AS
$BODY$
  BEGIN
    INSERT INTO store.import_set_messages
    (import_id,
     import_set_id,
     table_name,
     affected_columns,
     record_id,
     line_no,
     domain,
     message_no,
     message_level,
     message,
     detail)
   VALUES
    (p_import_id,
     p_import_set_id,
     p_table_name,
     p_affected_columns,
     p_record_id,
     p_line_no,
     p_domain,
     p_message_no,
     p_message_level,
     p_message,
     p_detail);
  END;
$BODY$
  LANGUAGE 'plpgsql';
  
CREATE TYPE store.import_message AS (
  message_no integer,
  affected_columns character varying(200),
  message_level integer,
  message character varying(4000),
  detail character varying(4000)
);

CREATE OR REPLACE FUNCTION store.make_import_message(p_message_level integer, p_message_no integer, p_message character varying, p_detail character varying, p_affected_columns character varying)
  RETURNS store.import_message AS
$BODY$
  DECLARE
    v_message store.import_message;
  BEGIN 
    v_message.message_no = p_message_no;
    v_message.affected_columns = p_affected_columns;
    v_message.message_level = p_message_level;
    v_message.message = p_message;
    v_message.detail = p_detail;
    RETURN v_message;
  END; 
$BODY$
  LANGUAGE 'plpgsql';
  
 
CREATE TABLE store.server_directory_sources
( source_name character varying(60) NOT NULL,
  directory_path character varying(200) NOT NULL);

CREATE TABLE store.input_source_types
( type_name character varying(30) NOT NULL,
  type_label character varying(30) NOT NULL);

insert into store.input_source_types (type_name, type_label) VALUES ('server_directory','Files from a server directory');

CREATE OR REPLACE VIEW store.all_input_sources AS 
SELECT a.source_type as source_type_name, b.type_label as source_type_label, a.source_name
FROM
(SELECT 'server_directory'::character varying as source_type, source_name
FROM store.server_directory_sources s) AS a, store.input_source_types b
WHERE a.source_type = b.type_name;

--insert into store.input_source_types (type_name, type_label) VALUES ('server_directory','Files from a server directory');

