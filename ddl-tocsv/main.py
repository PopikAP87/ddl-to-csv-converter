import csv
from pathlib import Path
from simple_ddl_parser import DDLParser

SOURCE_DIR = '../ddl-source/'
TARGET_DIR = '../csv-target/'


def get_row_constraints(statement_constraints, row):
    constraints = []
    row_name = row['name']
    for pk in statement_constraints['primary_keys']:
        if row_name in pk['columns']:
            constraints.append('PK')
    for fk in statement_constraints['references']:
        if row_name == fk['name']:
            constraints.append('FK')
    if row['nullable'] is True:
        constraints.append('NOT NULL')
    if row['unique'] is True:
        constraints.append('UNIQUE')
    if row['default'] is not None:
        constraints.append('DEFAULT ' + row['default'])
    constraints = list(dict.fromkeys(constraints))
    result = "\n".join(str(element) for element in constraints)
    return result


def get_row_type_with_len(row):
    if row['size'] is not None:
        result = '{}({})'.format(row['type'], str(row['size']))
    else:
        result = row['type']
    return result


def get_row_check_list(row):
    if row['check'] is not None:
        result = 'Возможные значения: {}'.format(row['check'])
    else:
        result = ''
    return result


# Находим все sql файлы в SOURCE_DIR
ddlFilesList = [f for f in Path().glob(SOURCE_DIR + "*.sql")]

for filePath in ddlFilesList:
    # Чтение и парсинг файла
    ddlFile = open(filePath, 'r').read()
    ddlParsList = DDLParser(ddlFile).run(output_mode="postgres")
    print('Read SQL file: {}'.format(filePath.name))
    if ddlParsList is not None:
        for statement in ddlParsList:
            # Создание данных для записи
            firs_row = ['Имя поля', 'Тип', 'Описание', 'Ограничения']
            csv_data = [firs_row]
            for row in statement['columns']:
                row_constraint = get_row_constraints(statement['constraints'], row)
                row_type_with_len = get_row_type_with_len(row)
                row_check_list = get_row_check_list(row)
                csv_data.append([row['name'], row_type_with_len, row_check_list, row_constraint])
            # Запись файла csv для каждой таблицы
            with Path('{}{}_{}.csv'.format(TARGET_DIR, statement['schema'], statement['table_name'])).open('w', newline='') as outfile:
                writer = csv.writer(outfile, quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
                writer.writerows(csv_data)
            print('Write CSV file: {}_{}.csv'.format(statement['schema'], statement['table_name']))
            print('=====================\n')