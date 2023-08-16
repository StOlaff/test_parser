import utils as util
import psycopg2 as pg

import tables


# конфиг БД и подключение
class DbSub:

    # параметры подключения к БД
    __dbName = 'postgres'
    __userName = 'postgres'
    __passWord = 'postgres'
    __host = 'localhost'
    __port = '5432'
    __connection = pg.connect(dbname=__dbName, user=__userName,
                              password=__passWord, host=__host, port=__port)

    # объект курсора
    __cursor = __connection.cursor()

    def __init__(self):
        self = self.getCursor()

    """
    Базовые методы: открытие курсора, исполнение запроса, закрытие курсора
    """
    def getCursor(self):
        return self.__cursor

    def execute(self, query, data=None):
        cursor = self.getCursor()
        if data is None:
            cursor.execute(query)
        else:
            cursor.execute(query, data)
        self.__connection.commit()

        return cursor

    def die(self):
        self.__connection.close()
        self.__cursor.close()

    """
    Основные методы
    """

    def create_table(self, table_inst):
        query = f'CREATE TABLE IF NOT EXISTS {table_inst.table_name}'
        columns = ', '.join([f'{x} {table_inst.columns.get(x)}' for x in table_inst.columns])

        if 'outer_keys' in dir(table_inst):
            keys = ', '.join([f' FOREIGN KEY ({x}) REFERENCES {table_inst.outer_keys.get(x)}'
                              for x in table_inst.outer_keys])
            columns += f', {keys}'

        query += f' ({columns})'

        self.execute(query)

    # создадим индексы по полю name для обеих таблиц
    def create_names_index(self, table_inst):
        query = f'CREATE UNIQUE INDEX IF NOT EXISTS {table_inst.table_name}_names ON {table_inst.table_name} (name)'
        self.execute(query)

    @staticmethod
    def insert_authors(data, table_inst):
        values = ', '.join(f'(\'{x}\')' for x in data)
        query = f"INSERT INTO author_table (name) VALUES {values} " \
                f"ON CONFLICT (name) DO UPDATE SET id = author_table.id " \
                f"RETURNING name, id;"

        return DbSub.execute(DbSub(), query)

    def insert_articles(self, data, table_inst):
        columns = ', '.join([f'{x}' for x in data[0].keys()])
        values_pack = []
        for i in data:
            values_pack.append(', '.join(f"'{x}'" for x in i.values()))

        values = ', '.join(f'({x})' for x in values_pack)

        query = f"INSERT INTO {table_inst.table_name} ({columns}) VALUES {values} " \
                f"ON CONFLICT (name) DO NOTHING;"

        return self.execute(query)

    def read_data_by_time(self, last_run):
        articles = tables.ArticleTable.table_name
        authors = tables.AuthorTable.table_name

        # working uncomment on prod
        query = f"SELECT * FROM {articles} INNER JOIN {authors} " \
                f"ON {articles}.author_id = {authors}.id " \
                f"WHERE {articles}.updated_at = '{last_run}' " \
                f"ORDER BY article_published ASC;"

        return self.execute(query).fetchall()
