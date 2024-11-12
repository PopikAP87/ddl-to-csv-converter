import csv
from pathlib import Path
from simple_ddl_parser import DDLParser

SOURCE_DIR = '../ddl-source/'
TARGET_DIR = '../csv-target/'
ERD_START = '''
@startuml

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
ERD_END = '''
@enduml
'''
ERD_SCHEMA = '''
$schema("{name}", "{alias}") '''
ERD_SCHEMA_END = '''
}
'''
ERD_TABLE = '''
    $table("{name}", "{alias}") '''
ERD_TABLE_END = '''
    }
'''
ERD_COLUMN = '''
        ${type}("{name}"): {data_type} {constrains}'''
ERD_RELATION_FROM = '''
{source_schema}.{source_table}::{source_column}'''
ERD_RELATION_TO = '''{target_schema}.{target_table}::{target_column} : {fk_title}
'''
ERD_RELATION_LINE = ' ||--o{ '


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
    if column['nullable'] is True:
        constraints.append('NOT NULL')
    if column['unique'] is True:
        constraints.append('UNIQUE')
    if column['default'] is not None:
        constraints.append('DEFAULT ' + column['default'])
    constraints = list(dict.fromkeys(constraints))
    result = "\n".join(str(element) for element in constraints)
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
             get_row_constraints(statement['constraints'], row)]
        )
    return result


# Получение распршеных выражений для всех файлов
def get_all_statements(list_of_sql_files):
    result = []
    for file_path in list_of_sql_files:
        ddl_file = open(file_path, 'r').read()
        ddl_pars_list = DDLParser(ddl_file).run(output_mode="postgres")
        if len(ddl_pars_list) == 0:
            print('!!! Error pars SQL from file: {}'.format(file_path.name))
            return False
        for statement in ddl_pars_list:
            result.append(statement)
    return result


# Создание csv файлов по ddl запросам в SOURCE_DIR
def write_csv(sql_statements):
    for statement in sql_statements:
        csv_data = create_csv_data(statement)
        with Path('{}{}_{}.csv'.format(TARGET_DIR,
                                       statement['schema'],
                                       statement['table_name']
                                       )
                  ).open('w', newline='') as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
            writer.writerows(csv_data)
        print('Write CSV file: {}_{}.csv'.format(statement['schema'], statement['table_name']))
        print('=====================\n')


def write_erd(sql_statements):
    erd = ERD_START
    # for statement in sql_statements:
    erd += ERD_SCHEMA.format(name='sch_name', alias='sch_name') + '{\n'
    erd += ERD_TABLE.format(name='t_name', alias='t_name') + '{'
    erd += ERD_COLUMN.format(type='pk', name='id', data_type='text', constrains='')
    erd += ERD_COLUMN.format(type='pfk', name='name', data_type='text', constrains='NOT NULL')
    erd += ERD_COLUMN.format(type='column', name='created', data_type='timestampz', constrains='DEFAULT now()')
    erd += ERD_TABLE_END
    erd += ERD_SCHEMA_END
    erd += ERD_RELATION_FROM.format(source_schema='sch_name', source_table='t_name', source_column='name')
    erd += ERD_RELATION_LINE
    erd += ERD_RELATION_TO.format(target_schema='sch_name', target_table='t_name', target_column='pk', fk_title='fk_test')
    erd += ERD_END
    with Path('{}erd.puml'.format(TARGET_DIR)).open('w', encoding='utf-8') as outfile:
        outfile.write(erd)


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
# write_csv(statements)
write_erd(statements)
