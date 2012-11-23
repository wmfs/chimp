-- Create PINHEAD schema
-- =====================

CREATE SCHEMA pinhead;
COMMENT ON SCHEMA pinhead IS 'A schema to be used for rendering geographical point data';

CREATE TYPE pinhead.pin_result AS
   (pin_name character varying(200),   
    key character varying(200),
    document_type character varying(200),
    x numeric (9,2),
    y numeric (9,2),
    distance numeric (9,2),
    icon character varying(200),
    label character varying(200),
    visibility integer,
    security integer);
    
CREATE OR REPLACE FUNCTION pinhead.convert_cql_to_sql(p_cql character varying) RETURNS character varying AS
$BODY$
  DECLARE
    v_sql character varying(4000);
  BEGIN
    v_sql = p_cql;
    RETURN v_sql;
  END;
$BODY$
 LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pinhead.get_pins_in_vicinity(p_x numeric, p_y numeric, p_radius integer, p_pin_names character varying, p_cql_array character varying = NULL)
  RETURNS SETOF pinhead.pin_result AS
$BODY$
  DECLARE
    v_circle geometry;
    v_cql_clauses character varying(2000)[];
    v_where_clause character varying(2000);
    v_pin_names character varying(200)[];
    v_result pinhead.pin_result;
  BEGIN 
    IF p_cql_array IS NOT NULL THEN
      v_cql_clauses = p_cql_array;
    END IF;
    v_pin_names = string_to_array(p_pin_names,',');
    v_circle = ST_Buffer(ST_GeomFromText('POINT('||p_x||' '||p_y||')',27700), p_radius);
    FOR i IN array_lower(v_pin_names,1)..array_upper(v_pin_names,1) LOOP
      IF p_cql_array IS NOT NULL THEN
        v_where_clause = pinhead.convert_cql_to_sql(v_cql_clauses[i]);
      ELSE
        v_where_clause = NULL;
      END IF;
      FOR v_result IN EXECUTE 'select * from pinhead.get_'||v_pin_names[i]||'_pins_in_area($1,$2)' USING v_circle, v_where_clause LOOP
        v_result.distance = shared.get_distance(p_x,p_y,v_result.x,v_result.y);
        RETURN NEXT v_result;
      END LOOP;
      
    END LOOP;
  END; 
$BODY$
  LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION pinhead.get_pins_in_vicinity(p_x float, p_y float, p_radius integer, p_pin_names character varying, p_cql_array character varying = NULL)
  RETURNS SETOF pinhead.pin_result AS
$BODY$
  DECLARE
    v_circle geometry;
    v_cql_clauses character varying(2000)[];
    v_where_clause character varying(2000);
    v_pin_names character varying(200)[];
    v_result pinhead.pin_result;
  BEGIN 
    IF p_cql_array IS NOT NULL THEN
      v_cql_clauses = p_cql_array;
    END IF;
    v_pin_names = string_to_array(p_pin_names,',');
    v_circle = ST_Buffer(ST_GeomFromText('POINT('||p_x||' '||p_y||')',27700), p_radius);
    FOR i IN array_lower(v_pin_names,1)..array_upper(v_pin_names,1) LOOP
      IF p_cql_array IS NOT NULL THEN
        v_where_clause = pinhead.convert_cql_to_sql(v_cql_clauses[i]);
      ELSE
        v_where_clause = NULL;
      END IF;
      FOR v_result IN EXECUTE 'select * from pinhead.get_'||v_pin_names[i]||'_pins_in_area($1,$2)' USING v_circle, v_where_clause LOOP
        v_result.distance = shared.get_distance(p_x,p_y,v_result.x,v_result.y);
        RETURN NEXT v_result;
      END LOOP;
      
    END LOOP;
  END; 
$BODY$
  LANGUAGE plpgsql;
