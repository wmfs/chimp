
-- Create AREAS schema
-- ===================

CREATE SCHEMA areas;
COMMENT ON SCHEMA areas IS 'A schema to store spatial areas.';

CREATE TYPE areas.area_info AS
   (area_name character varying(30),
    polygon_label character varying(200),
    polygon_id character varying(30));

CREATE TYPE areas.grid_square AS
   (id integer,
    min_x integer,
    min_y integer,
    max_x integer,
    max_y integer,
    origin_x integer,
    origin_y integer,
    square geometry);


CREATE OR REPLACE FUNCTION areas.generate_grid_squares(p_min_x integer, p_min_y integer, p_max_x integer, p_max_y integer, p_square_size integer, p_srid integer)
  RETURNS SETOF areas.grid_square AS
$BODY$  
DECLARE
  v_square areas.grid_square;
  v_id integer;
BEGIN

  v_id = 1;
  FOR y IN p_min_y..p_max_y BY p_square_size LOOP
    FOR x IN p_min_x..p_max_x BY p_square_size LOOP
      v_square.id = v_id;
      v_square.min_x = x;
      v_square.min_y = y;
      v_square.max_x = x + p_square_size;
      v_square.max_y = y + p_square_size;
      v_square.origin_x = x + (p_square_size/2);
      v_square.origin_y = y + (p_square_size/2);                       
      v_square.square = ST_GeometryFromText('Polygon (('||x||' '||y||','||x||' '||y+p_square_size||','||x+p_square_size||' '||y+p_square_size||','||x+p_square_size||' '||y||','||x||' '||y||'))', p_srid);
      v_id = v_id +1;
      RETURN NEXT v_square;
    END LOOP;
  END LOOP;

END;
$BODY$
  LANGUAGE 'plpgsql' IMMUTABLE STRICT;
     
CREATE OR REPLACE FUNCTION areas.get_area_info(p_x numeric, p_y numeric, p_area_names character varying)
  RETURNS SETOF areas.area_info AS
$BODY$
  DECLARE
    v_sql character varying(200);
    v_point geometry;
    v_area_names character varying(200)[];
    v_result areas.area_info;
  BEGIN 
    v_area_names = string_to_array(p_area_names,',');
    v_point = ST_GeomFromText('POINT('||p_x||' '||p_y||')',27700);
    FOR i IN array_lower(v_area_names,1)..array_upper(v_area_names,1) LOOP
        v_sql := 'SELECT * FROM areas.get_points_' || v_area_names[i]||'($1)';
	EXECUTE v_sql
	INTO v_result
	USING v_point;
	RETURN NEXT v_result;    
      END LOOP;     
  END; 
$BODY$
  LANGUAGE 'plpgsql';


