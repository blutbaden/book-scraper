import logging
import os
import urllib

import pandas as pd
import os.path
from os import path

from tqdm import tqdm
import requests
import bs4
import json
from datetime import datetime


class BookDetailsScraper:

    def __init__(self):
        self.output = None
        self.current_date = datetime.now().date().strftime("%Y-%m-%d")
        for dir_name in ['./data', './data/images', './data/pdf']:
            try:
                os.stat(dir_name)
            except:
                os.mkdir(dir_name)

    def init_fields(self):
        field_genre = []
        published = ''
        pages = ''
        description = ''
        isbn = ''
        download_url = ''
        return description, download_url, field_genre, isbn, pages, published

    def parse_data(self, books, description, download_url, field_genre, i, isbn, pages, parser, published):
        title = books.iloc[i][0].split(' by ')[0]
        author = books.iloc[i][0].split(' by ')[1]
        image = books.iloc[i][1]
        # To save each book image in disk uncomment this line bellow
        # urllib.request.urlretrieve(image, './data/images/%s.jpg' % (title))
        try:
            pages = parser.find('div',
                                class_="field field--name-field-pages field--type-integer field--label-hidden field--item").text.strip()
        except Exception as e:
            pass
        try:
            description = parser.find('div',
                                      class_="field field--name-field-description field--type-string-long field--label-hidden field--item").text.strip()
        except Exception as e:
            try:
                description = parser.find('div',
                                          class_="field field--name-field-excerpt field--type-text-long field--label-hidden field--item").text.strip()
            except Exception as e:
                pass
        try:
            field_genre_elm = parser.find('div',
                                          class_="field field--name-field-genre field--type-entity-reference field--label-hidden field--items").find_all(
                "a")
            for elm in field_genre_elm:
                field_genre.append(elm.text.strip())
        except Exception as e:
            pass
        try:
            published = parser.find('div',
                                    class_="field field--name-field-published-year field--type-integer field--label-hidden field--item").text.strip()
        except Exception as e:
            pass
        try:
            isbn = parser.find('div',
                               class_="field field--name-field-isbn field--type-string field--label-hidden field--item").text.strip()
        except Exception as e:
            pass
        try:
            download_url = \
                parser.find('a', class_="mb-link-files use-ajax mb-login-ajax-link")['href'].split("=")[1]
            download_url = 'https://library.manybooks.net/live/get-book/' + download_url + '/pdf'
            # To save each book as pdf in disk uncomment this line bellow
            # urllib.request.urlretrieve(download_url, './data/pdf/%s.pdf' % (title))
        except Exception as e:
            pass
        return author, description, download_url, image, isbn, pages, published, title

    def scrap_date(self):
        total_details = 0
        headers = open("headers.json")
        headers = json.load(headers)
        header = headers[1]
        # get details file if exist
        if path.exists('./data/books-details.csv'):
            df = pd.read_csv('./data/books-details.csv', index_col=0)
            total_details = len(df)
        else:
            df = pd.DataFrame(
                columns=['title', 'author', 'isbn', 'image', 'published', 'pages', 'description', 'field_genre',
                         'download_url'])
        # get books csv file
        if path.exists('./data/books.csv'):
            books = pd.read_csv('./data/books.csv', index_col=0)
            # creating a list of dataframe columns
            total_books = len(books)
            pbar = tqdm(total=total_books, initial=total_details)
            for i in range(total_details, total_books):
                logging.info('Request page #%i...' % i)
                description, download_url, field_genre, isbn, pages, published = self.init_fields()
                url = books.iloc[i][2]
                response = requests.get(url, headers=header)
                logging.info('Grab page #%i...' % i)
                # Parses the html output
                parser = bs4.BeautifulSoup(response.content, 'html.parser')
                # Parse all the information
                author, description, download_url, image, isbn, pages, published, title = self.parse_data(books, description, download_url, field_genre, i, isbn, pages, parser, published)
                book = json.dumps(
                    {'title': title, 'author': author, 'isbn': isbn, 'image': image, 'published': published,
                     'pages': pages, 'description': description, 'field_genre': field_genre,
                     'download_url': download_url})
                book_details = pd.DataFrame([eval(book)])
                df = df.append(book_details).reset_index(drop=True)
                df.to_csv('./data/books-details.csv')
                df = pd.read_csv('./data/books-details.csv', index_col=0)
                pbar.update(1)
            pbar.close()
            return False
        return False

if __name__ == '__main__':
    BookDetailsScraper().scrap_date()
