import csv
from pathlib import Path
from simple_ddl_parser import DDLParser

SOURCE_DIR = '../ddl-source/'
TARGET_DIR = '../csv-target/'
TARGET_TABLE_DIR = TARGET_DIR + 'tables/'
ERD_START = '''@startuml

!theme plain
hide empty methods

!procedure $schema($name, $slug)
package "$name" as $slug <<Rectangle>>
!endprocedure

!procedure $table($name, $slug)
entity "<b>$name</b>" as $slug << (T, Orange) table >>
!endprocedure

!procedure $view($name, $slug)
entity "<b>$name</b>" as $slug << (V, Aquamarine) view >>
!endprocedure

!procedure $pk($name)
<color:#GoldenRod><&key></color> <b>$name</b>
!endprocedure

!procedure $fk($name)
<color:#Silver><&key></color> $name
!endprocedure

!procedure $pfk($name)
<color:#GoldenRod><&key></color><color:#Silver><&key></color> <b>$name</b>
!endprocedure

!procedure $column($name)
{field} <color:#White><&media-record></color> $name
!endprocedure
'''
DB_DOC_HEADER = '''= БД {db_name}
:db-name: <Заполнить название базы>
:service-dir: ./
ifndef::env-site[]
:toc-title: Содержание
:toc:

.Описание БД
****
Тут будет описание БД
****

== ERD БД
[plantuml, class, format=svg, Interactive, opts=interactive]
----
include::{service_dir}db_erd.puml[]
----
'''
DB_DOC_SCHEMA = '''
== Схема {schema_name}
TODO схемы {schema_name}
'''
DB_DOC_TABLE = '''
=== {schema_name}.{table_name}
TODO описание таблицы {table_name}

.Раскрыть
[%collapsible]
====
[%header,format=csv,cols="1a, 1a, 1a, 1a"]
|===
include::{service_dir}/tables/{schema_name}_{table_name}.csv[]
|===
====
'''
log_num = 1


# Получение констрейнов для колонки
def get_row_constraints(statement_constraints, column):
    constraints = []
    column_name = column['name']
    if 'primary_keys' in statement_constraints:
        for pk in statement_constraints['primary_keys']:
            if column_name in pk['columns']:
                constraints.append('PK')
    if 'references' in statement_constraints:
        for fk in statement_constraints['references']:
            if column_name == fk['name']:
                constraints.append('FK')
    if column['nullable'] is False:
        constraints.append('NOT NULL')
    if column['unique'] is True:
        constraints.append('UNIQUE')
    if column['default'] is not None:
        constraints.append('DEFAULT ' + column['default'])
    result = list(dict.fromkeys(constraints))
    return result


# Получение типа данных с длиной для колонки
def get_row_type_with_len(column):
    if column['size'] is not None:
        result = '{}({})'.format(column['type'], str(column['size']))
    else:
        result = column['type']
    return result


# Получение возможных значений для колонки
def get_row_check_list(column):
    if column['check'] is not None:
        result = 'Возможные значения: {}'.format(column['check'])
    else:
        result = ''
    return result


# Создание данных для записи в csv описания таблицы
def create_csv_data(statement):
    result = [['Имя поля', 'Тип', 'Описание', 'Ограничения']]
    for row in statement['columns']:
        result.append(
            [row['name'],
             get_row_type_with_len(row),
             get_row_check_list(row),
             '\n'.join(str(e) for e in get_row_constraints(statement['constraints'], row))]
        )
    return result


# Получение распршеных выражений для всех файлов
def get_all_statements(list_of_sql_files):
    result = []
    for file_path in list_of_sql_files:
        ddl_file = open(file_path, 'r').read()
        ddl_pars_list = DDLParser(ddl_file, normalize_names=True).run(output_mode="postgres")
        if len(ddl_pars_list) == 0:
            print('!!! Error pars SQL from file: {}'.format(file_path.name))
            return False
        for statement in ddl_pars_list:
            result.append(statement)
    return result


# Создание csv файлов по ddl запросам в SOURCE_DIR
def create_db_csv_files(sql_statements):
    global log_num
    Path(TARGET_TABLE_DIR).mkdir(parents=True, exist_ok=True)
    for statement in sql_statements:
        csv_data = create_csv_data(statement)
        with Path('{}{}_{}.csv'.format(TARGET_TABLE_DIR,
                                       statement['schema'],
                                       statement['table_name']
                                       )
                  ).open('w', newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
            writer.writerows(csv_data)
        print('{}. Create CSV file: {}_{}.csv'.format(log_num, statement['schema'], statement['table_name']))
        log_num += 1
        print('=====================\n')


def get_db_erd_table(sql_statement):
    erd_table = '\n    $table("{name}", "{alias}") '
    erd_table_end = '\n    }\n'
    erd_column = '\n        ${type}("{name}"): {data_type} {constrains}'
    table = erd_table.format(
        name=sql_statement['table_name'],
        alias=sql_statement['table_name']
    ) + '{'
    for column in sql_statement['columns']:
        constrains = get_row_constraints(sql_statement['constraints'], column)
        if 'PK' in constrains and 'FK' not in constrains:
            c_type = 'pk'
        elif 'PK' not in constrains and 'FK' in constrains:
            c_type = 'fk'
        elif 'PK' in constrains and 'FK' in constrains:
            c_type = 'pfk'
        else:
            c_type = 'column'
        table += erd_column.format(
            type=c_type,
            name=column['name'],
            data_type=get_row_type_with_len(column),
            constrains=' '.join(str(e) for e in constrains)
        )
    table += erd_table_end
    result = table
    return result


def get_db_erd_relations(sql_statements):
    erd_relation_from = '\n{source_schema}.{source_table}::{source_column}'
    erd_relation_to = '{target_schema}.{target_table}::{target_column} : {fk_title}\n'
    erd_relation_line = ' ||--o{ '
    relations = ''
    for statement in sql_statements:
        if 'references' in statement['constraints']:
            for relation in statement['constraints']['references']:
                relations += erd_relation_from.format(
                    source_schema=statement['schema'],
                    source_table=statement['table_name'],
                    source_column=relation['name']
                )
                relations += erd_relation_line
                relations += erd_relation_to.format(
                    target_schema=relation['schema'],
                    target_table=relation['table'],
                    target_column=relation['columns'][0],
                    fk_title=relation['constraint_name']
                )
    result = relations
    return result


def create_db_erd_file(sql_statements):
    global log_num
    erd = ERD_START
    erd_schema = '\n$schema("{name}", "{alias}") '
    schemas = list(dict.fromkeys([item['schema'] for item in sql_statements]))
    for schema in schemas:
        erd += erd_schema.format(name=schema, alias=schema) + '{\n'
        schema_tables = [item for item in sql_statements if item['schema'] == schema]
        for table in schema_tables:
            erd += get_db_erd_table(table)
        erd += '\n}\n'
    erd += get_db_erd_relations(sql_statements)
    erd += '\n@enduml'
    with Path('{}db_erd.puml'.format(TARGET_DIR)).open('w', encoding='utf-8') as outfile:
        outfile.write(erd)
    print('{}. Create DB ERD file: db_erd.puml'.format(log_num))
    log_num += 1
    print('=====================\n')


def create_db_doc_file(sql_statements):
    global log_num
    db_doc = DB_DOC_HEADER.format(db_name='{db-name}',
                                  service_dir='{service-dir}')
    schemas = list(dict.fromkeys([item['schema'] for item in sql_statements]))
    for schema in schemas:
        db_doc += DB_DOC_SCHEMA.format(schema_name=schema)
        schema_tables = [item for item in sql_statements if item['schema'] == schema]
        for table in schema_tables:
            db_doc += DB_DOC_TABLE.format(schema_name=schema,
                                          table_name=table['table_name'],
                                          service_dir='{service-dir}')
    with Path('{}db_doc.adoc'.format(TARGET_DIR)).open('w', encoding='utf-8') as outfile:
        outfile.write(db_doc)
    print('{}. Create DB doc file: db_doc.adoc'.format(log_num))
    log_num += 1
    print('=====================\n')


# Создаем TARGET_DIR и находим все sql файлы в SOURCE_DIR
Path(TARGET_DIR).mkdir(parents=True, exist_ok=True)
list_of_files = [f for f in Path().glob(SOURCE_DIR + "*.sql")]
if len(list_of_files) == 0:
    print('No sql files in directory: {}'.format(SOURCE_DIR))
    exit()
statements = get_all_statements(list_of_files)
if not statements:
    print('No sql statement in directory: {}'.format(SOURCE_DIR))
    exit()
create_db_csv_files(statements)
create_db_erd_file(statements)
create_db_doc_file(statements)
