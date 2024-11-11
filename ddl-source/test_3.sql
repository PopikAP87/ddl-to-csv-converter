CREATE TABLE schema.test_table_3 (
   id integer PRIMARY KEY,
   name varchar(30) NOT NULL,
   ext_id text,
   description text,
   created timestamp,
   status varchar(30) NOT NULL,
   used bool DEFAULT false,
   CONSTRAINT tt_pk_1 PRIMARY KEY (id, ext_id),
   CONSTRAINT fk_ext_id_1 FOREIGN KEY (ext_id) REFERENCES schema.test_table_2(id)
);