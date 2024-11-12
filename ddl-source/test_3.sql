CREATE TABLE schema_3.test_table_5 (
   id integer,
   name varchar(30) NOT NULL,
   ext_id text,
   description text,
   created timestamp,
   status varchar(30) NOT NULL,
   used bool DEFAULT false,
   CONSTRAINT tt_pk_441 PRIMARY KEY (id, ext_id),
   CONSTRAINT fk_ext_id_16575 FOREIGN KEY (ext_id) REFERENCES schema_2.test_table_7(id)
);