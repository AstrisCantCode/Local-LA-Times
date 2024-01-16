from random import randint
from multiprocessing import cpu_count
from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
from func_timeout import func_set_timeout

sitemapURL = 'https://latimes.com/sitemap'

# I actually don't know if this is necessary (the random UA stuff)
UAstrings = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.2227.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.3497.92 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
]

def getHeader():
    return {'User-Agent': UAstrings[randint(0, len(UAstrings)-1)]}

def get_num_proc():
    return cpu_count() * 6

def get_current_ym():
    current = datetime.now()
    return current.year, current.month

@func_set_timeout(15)
def parsed_from_url(url):
    result = requests.get(url, headers=getHeader())
    return bs(result.text, 'lxml')