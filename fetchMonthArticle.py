from webbrowser import get
import requests
from bs4 import BeautifulSoup as bs
from .utils import getHeader, sitemapURL, parsed_from_url, get_num_proc
from .fetchArticle import articleFetch
from datasets import Dataset
from multiprocessing import Pool

def monthArticlesFetch(year, month):

    parsed = parsed_from_url(f'{sitemapURL}/{str(year)}/{str(month)}')

    pages = [x.a['href'] for x in parsed.find(class_='archive-page-pagination-menu').children if x != " "]

    dataset_dict = {
        'title': [],
        'url': []
    }

    with Pool(get_num_proc()) as pool:
        parsed_pages = pool.map(parsed_from_url, pages)

    for page in parsed_pages:
        page_menu = page.find(class_='archive-page-menu')
        if page_menu != None:
            for x in page_menu.children:
                if (x == " ") or (x.a == None):
                    continue
                title = x.text.strip(' ')
                dataset_dict['title'].append(title)
                dataset_dict['url'].append(x.a['href'])
    
    dataset = Dataset.from_dict(dataset_dict)

    dataset = dataset.map(articleFetch, num_proc=get_num_proc(), keep_in_memory=True)

    return dataset
