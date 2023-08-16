from datetime import datetime
import time

from urllib import request as url_req
from bs4 import BeautifulSoup
import schedule

import utils as util
from db_sub import DbSub
from tables import AuthorTable, ArticleTable

# время последнего запуска, по нему будем фильтровать данные для вставки в отчет
last_run = ''


# готовим БД, создаем таблицы
def prepare_db():
    db = DbSub()
    db.create_table(AuthorTable)
    db.create_names_index(AuthorTable)
    db.create_table(ArticleTable)
    db.create_names_index(ArticleTable)


# возвращает словарь со списками данных
def scrape_data():
    domain = 'https://habr.com'

    author_values = []
    article_values = []

    for i in range(1, 6):
        # адреса для локальной отладки
        # target_url = f'http://localhost/page{i}.html'

        target_url = domain + f'/ru/articles/page{i}/'

        request = url_req.urlopen(target_url, data=None, timeout=1.0)

        # получаем декодированый html текст
        html_text = request.read().decode('utf-8')
        soup = BeautifulSoup(html_text, 'lxml')

        # список всех статей на странице
        article_list = soup.find_all('div', {'class': "tm-article-snippet tm-article-snippet"})


        while len(article_list) > 0:
            article = article_list.pop()

            # парсим html, готовим данные
            author_row = article.findNext('a', {'class': "tm-user-info__username"}).text.strip()\
                .replace("'", '`').replace('"', '`')

            article_row = {
                'name': article.findNext('a', {'class': "tm-title__link"}).text.replace("'", '`').replace('"', '`'),
                'article_published': article.findNext('time').attrs.get('datetime'),
                'link':  domain + article.findNext('a', {'class': 'tm-article-snippet__readmore'}).attrs.get('href'),
                'author_name': article.findNext('a', {'class': "tm-user-info__username"}).text.strip()
                .replace("'", '`').replace('"', '`'),
                'article_text': article.findNext('div', {'class': 'tm-article-body tm-article-snippet__lead'}).text
                .replace("'", '`').replace('"', '`'),
                'updated_at': last_run
            }

            # результирующие списки с данными
            author_values.append(author_row)
            article_values.append(article_row)

    return {'authors': set(author_values), 'articles': article_values}


# записываем данные
def insert_data():
    global last_run
    last_run = datetime.now().strftime('%y-%m-%d %H:%M:%S')
    data = scrape_data()
    authors = data.pop('authors')
    articles = data.pop('articles')

    db = DbSub()
    # записываем данные авторов и получаем id записей для связи таблиц
    author_db_response = list(db.insert_authors(authors, AuthorTable))

    ar_author = {}
    for row in author_db_response:
        ar_author[row[0]] = row[1]

    """
    не перебираем список напрямую специально
    так как нам нужен порядковый номер элемента списка статей 
    """

    for art in articles:
        art['author_id'] = ar_author.get(art['author_name'])
        art.pop('author_name')

    # записываем данные статей
    db.insert_articles(articles, ArticleTable)


def read_data():
    db = DbSub()
    global last_run
    return db.read_data_by_time(last_run)


def report_data():
    keys = list(ArticleTable.columns.keys())
    keys.append('author_name')
    db_data = read_data()
    data = []
    while len(db_data) > 0:
        row = list(db_data.pop(0))
        row.pop(len(row)-2)
        data.append(row)

    util.write_data(last_run, data, keys)


def run_parser():
    prepare_db()
    insert_data()
    report_data()
    print('run successful', datetime.now())


def scheduler():
    #первый запуск
    run_parser()

    schedule.every(5).hours.do(run_parser)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    scheduler()
