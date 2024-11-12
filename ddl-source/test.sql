CREATE TABLE schema_1.test_table_1 (
   id integer,
   name varchar(30) NOT NULL,
   ext_id text,
   description text,
   created timestamp,
   status varchar(30) NOT NULL,
   used bool DEFAULT false,
   CONSTRAINT tt_pk_1 PRIMARY KEY (id, ext_id),
   CONSTRAINT fk_ext_id_1 FOREIGN KEY (ext_id) REFERENCES schema_1.test_table_2(id)
);

CREATE TABLE schema_1.test_table_2 (
   id integer,
   name varchar(30) NOT NULL,
   description text,
   CONSTRAINT tt_pk_123 PRIMARY KEY (id)
);

CREATE TABLE schema_1.test_table_3 (
   id integer,
   name varchar(30) NOT NULL,
   ext_id text,
   description text,
   created timestamp,
   status varchar(30) NOT NULL,
   used bool DEFAULT false,
   CONSTRAINT tt_pk_441 PRIMARY KEY (id, ext_id),
   CONSTRAINT fk_ext_id_16575 FOREIGN KEY (ext_id) REFERENCES schema_1.test_table_1(id)
);