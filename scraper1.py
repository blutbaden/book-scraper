import logging
import os
import pandas as pd
import os.path
from os import path

from tqdm import tqdm
import requests
import bs4
import json
import time
import random
from datetime import datetime


class BookScraper:

    def __init__(self, cat, language):
        cats = open("categories.json")
        cats = dict(json.load(cats))
        self.field_genre = cats[cat]
        self.language = language
        self.output = None
        self.current_date = datetime.now().date().strftime("%Y-%m-%d")
        self.currentPage = 0
        self.totalPages = 2660
        for dir_name in ['./data']:
            try:
                os.stat(dir_name)
            except:
                os.mkdir(dir_name)

    def make_url(self, page):
        url = f"https://manybooks.net/search-book?language={self.language}&page={page}"
        if self.field_genre != "":
            url += f"&field_genre[{self.field_genre}]={self.field_genre}"
        return url

    def check_request(self, response):
        if response.status_code != 200:
            if response.status_code == 403:
                logging.error('Security check not passed :(')
            elif response.status_code == 404:
                logging.error('Page not found!')
            return False

    def get_total_pages(self, headers):
        header = headers[1]
        response = requests.get(self.make_url(self.currentPage), headers=header)
        self.check_request(response)
        parser = bs4.BeautifulSoup(response.content, 'html.parser')
        last_item = parser.find("li", {"class": "pager__item pager__item--last"}).find("a")['href']
        last_item_filed = last_item.split('&')
        self.totalPages = int(last_item_filed[-1].split('=')[1])

    def scrap_date(self, headers):
        logging.info('Request page #%i...' % self.currentPage)
        # get csv file
        if path.exists('./data/books.csv'):
            df = pd.read_csv('./data/books.csv', index_col=0)
        else:
            df = pd.DataFrame(columns=['title', 'image', 'link'])
        books = []
        header = headers[1]
        # 10% chance to change agent
        if random.choice(range(100)) <= 10:
            header = random.choice(headers)
        # Sends a request
        response = requests.get(self.make_url(self.currentPage), headers=header)
        self.check_request(response)
        logging.info('Grab page #%i...' % self.currentPage)
        # Parses the html output
        parser = bs4.BeautifulSoup(response.content, 'html.parser')
        # Gathering all the information
        for elm in parser.find_all('div', class_="content"):
            images = elm.find_all("img", {"class": "img-responsive"})
            for img in images:
                link = img.parent
                book = json.dumps({'title': img['alt'], 'image': 'https://manybooks.net' + img['src'],
                                   'link': 'https://manybooks.net' + link['href']})
                books.append(eval(book))
        self.currentPage += 1
        elms = pd.DataFrame(books)
        df = df.append(elms).reset_index(drop=True)
        df.to_csv('./data/books.csv')
        if self.currentPage > self.totalPages:
            return False

    def send_request(self):
        flag = True
        # Headers to fake the request as browser to avoid blocking
        headers = open("headers.json")
        headers = json.load(headers)
        while True:
            try:
                # scrap data
                pbar = tqdm(total=self.totalPages)
                for i in range(self.totalPages):
                    self.scrap_date(headers)
                    pbar.update(1)
                pbar.close()
            except requests.ConnectionError:
                if flag:
                    logging.error('Connection lost! Waiting for connection...')
                    flag = False
                time.sleep(random.randint(1, 5))
                pass


if __name__ == '__main__':
    BookScraper("All", "All").send_request()
