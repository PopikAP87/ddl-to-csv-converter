import csv
from pathlib import Path
from simple_ddl_parser import DDLParser

SOURCE_DIR = '../ddl-source/'
TARGET_DIR = '../csv-target/'


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
    for statement in sql_statements:

        return False


# Находим все sql файлы в SOURCE_DIR
list_of_files = [f for f in Path().glob(SOURCE_DIR + "*.sql")]
if len(list_of_files) == 0:
    print('No sql files in directory: {}'.format(SOURCE_DIR))
    exit()
statements = get_all_statements(list_of_files)
if not statements:
    print('No sql statement in directory: {}'.format(SOURCE_DIR))
    exit()
write_csv(statements)
