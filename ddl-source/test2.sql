CREATE TABLE schema_2.test_table_7 (
   id integer NOT NULL,
   name text NOT NULL,
   ext_id text NOT NULL
   description text,
   created timestamp NOT NULL DEFAULT now(),
   updated timestamp,
   day_of_year int4 NOT NULL,
   CONSTRAINT tt_pk_2 PRIMARY KEY (id, day_of_year),
   CONSTRAINT fk_ext_id_2 FOREIGN KEY (ext_id) REFERENCES schema_1.test_table_1(id)
)
PARTITION BY LIST (day_of_year);
CREATE INDEX sfdf_idx ON ONLY schema.test_table_2 USING btree (created);