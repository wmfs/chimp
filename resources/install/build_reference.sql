
-- Create REFERENCE schema
-- =======================

CREATE SCHEMA reference;
COMMENT ON SCHEMA reference IS 'Schema to hold look-up data, geographical areas etc.';

CREATE TABLE reference.zones
(
  id serial NOT NULL,
  zone_seq smallint NOT NULL,
  buffer_distance integer NOT NULL,
  CONSTRAINT zone_pk PRIMARY KEY (id)
);

