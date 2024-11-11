import csv
import pathlib
from simple_ddl_parser import DDLParser

SOURCE_DIR = '../ddl-source/'
TARGET_DIR = '../csv-target/'

# Находим все ddl файлы в SOURCE_DIR
ddlFilesList = [f for f in pathlib.Path().glob(SOURCE_DIR + "*.sql")]
print("Read DDL files:")
for filePath in ddlFilesList:
    # Чтение и парсинг файла
    ddlFile = open(filePath, 'r').read()
    ddlParsList = DDLParser(ddlFile).run(output_mode="postgres")

    for statement in ddlParsList:
        # Запись файла csv для каждой таблицы
        with open('{}{}_{}.csv'.format(TARGET_DIR, statement['schema'], statement['table_name']), 'w') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(['Имя поля', 'Тип', 'Описание', 'Ограничения'])
            for row in statement['columns']:
                writer.writerow([row['name'], row['type'], '', ''])

        # Запись результатов чтения и парсинга файла
        print('   File name: ' + filePath.name)
        print('   DDL:\n' + ddlFile + '\n')
        print('   Table {}.{}:'.format(statement['schema'], statement['table_name']))
        print('   Column | DataType | Description')
        for col in statement['columns']:
            print('   {} | {}'.format(col['name'], col['type']))
        print("   End file \n")