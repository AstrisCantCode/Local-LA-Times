import requests
from bs4 import BeautifulSoup as bs
from .utils import get_num_proc, sitemapURL, parsed_from_url
from multiprocessing import Pool

def monthsFetch(year):
    parsed = parsed_from_url(f'{sitemapURL}/{str(year)}')

    months = [int(x.a['href'].split('/')[-1]) for x in parsed.find(class_='archive-page-menu').children if x != ' ']
    return {year: months}

#fetch year-month dict: a dict whos keys are years and whos values are (a list of) months available on the website. 
def ymdFetch():
    parsed = parsed_from_url(sitemapURL)

    ymd = dict.fromkeys([int(x.a['href'].split('/')[-1]) for x in parsed.find(class_='archive-page-menu archive-page-menu-horizontal').children if x != ' '], [])
    
    with Pool(get_num_proc()) as pool:
        [ymd.update(x) for x in pool.map(monthsFetch, ymd.keys())]

    #out: a list of tuples containing (year, month)
        
    return ymd