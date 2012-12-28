CREATE SCHEMA lana;

CREATE TABLE lana.import_template (
    specification_name character varying(30) NOT NULL,
    directory character varying(2000),
    limit_number integer,
    import_mode character varying(20),
    tolerance_level character varying(20),
    commit_frequency character varying(20),
    checkpoint_behaviour character varying(20),
    has_defer_processing boolean,
    has_recurse boolean,
    filename_regex character varying(200),
    group_id character varying(200),
    json character varying(200),
    vaccum_strategy character varying(20),
    post_import_compute character varying(20),
    is_favourite boolean,
    "position" integer);

