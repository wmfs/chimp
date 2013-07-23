
-- Create ALERTS schema
-- ====================

CREATE SCHEMA alerts;
COMMENT ON SCHEMA alerts IS 'A schema to help produce Chimp alerts.';

CREATE SEQUENCE alerts.global_seq START 1;

CREATE TABLE alerts.global_alerts
(
  id bigint NOT NULL DEFAULT nextval('alerts.global_seq'),
  domain character varying(30) NOT NULL,
  icon_filename character varying(200) NOT NULL,
  created timestamp with time zone NOT NULL DEFAULT now(),
  title character varying(200) NOT NULL,
  content character varying,
  action_json character varying,
  owned_by character varying(200)
);

CREATE INDEX alert_created_idx ON alerts.global_alerts(created);

CREATE INDEX alert_domain_idx ON alerts.global_alerts(domain);

CREATE INDEX alert_owned_by_idx ON alerts.global_alerts(owned_by);

