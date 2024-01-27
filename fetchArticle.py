import requests
from bs4 import BeautifulSoup as bs
import random
from func_timeout import func_set_timeout, FunctionTimedOut
from .utils import parsed_from_url

def getDateTime(parsedpage):
    dateTime = parsedpage.find(class_='published-date')
    if dateTime is not None:
        return dateTime['datetime']
    else:
        return ""

def getText(parsedpage):
    main_element = parsedpage.find(attrs={'data-element': 'story-body'})
    text = ""
    if main_element is not None:
        for child in main_element.findAll('p'):
            text += child.getText().replace('\n', "") + '\n\n'
    return text

def getArticleInfo(url):

    num_retries = 0
    failed = True
    while num_retries < 3 and failed == True:
        try:
            parsed = parsed_from_url(url)
            failed = False
        except:
            print('Function errored. Retrying.')
    if failed == True:
        print('Function failed after 3 attempts.')
        raise Exception()

    dateTime = getDateTime(parsed)
    text = getText(parsed)

    return {
        'author': "",
        'dateTime': dateTime,
        'text': text,
        'needs_collection': False
    }

def articleFetch(rowDict: dict) -> dict:
    try:
        newinfo = getArticleInfo(rowDict['url'])
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        print(f"Got an exception. url: {rowDict['url']}")
        newinfo = {'author': "", 'dateTime': "", 'text': "", 'needs_collection': True} # if it fails, it still must be collected (although perhaps later)
    rowDict.update(newinfo)
    return rowDict
