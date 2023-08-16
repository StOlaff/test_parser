"""
Классы таблиц. Описаны названия таблиц, внешние ключи, поля и их типы
Колонки описываются в формате: column_name: column_type
Внешние ключи: {column_name: reference_table_name(outer_column_name)}
"""


class AuthorTable:
    table_name = 'author_table'

    columns = {
        'id': 'SERIAL PRIMARY KEY',
        'name': 'VARCHAR(30)',
    }


class ArticleTable:
    table_name = 'article_table'

    columns = {
        'id': 'SERIAL PRIMARY KEY',
        'name': 'TEXT',
        'article_text': 'TEXT',
        'article_published': 'TIMESTAMP',
        'link': 'TEXT',
        'author_id': 'INTEGER',
        'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP(0)'
    }

    outer_keys = {'author_id': 'author_table(id)'}
