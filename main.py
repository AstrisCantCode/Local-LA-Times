from readline import get_current_history_length
from datasets import Dataset, load_from_disk
from bs4 import BeautifulSoup as bs
from .utils import get_current_ym, get_num_proc
from .fetchMonthArticle import monthArticlesFetch
import os
import datasets
import shutil
from .fetchYMD import ymdFetch
# ympFetch takes no arguments and returns Year+Month pairs available in the archive, as a list of tuples.

from .fetchArticle import articleFetch
# articleFetch takes a dataset row (dict), fetches ['author', 'dateTime', 'text'] using ['url'], updates the dict, 
# and returns it, setting 'needs_collection' to false if it successfully fetches the article. (Still runs if needs_collection=False!)

datasets.disable_progress_bars()

package_path = os.path.dirname(os.path.realpath(__file__))

def db_init():
    
    print('Fetching list of available years and months from the sitemap...')

    ymd = ymdFetch()

    print('Finished fetching years and months. Making folders locally...')

    os.mkdir("/".join([package_path, 'database']))

    current_year, current_month = get_current_ym()

    for year in ymd.keys():
        print(f'Getting Data for {year}')

        os.mkdir("/".join([package_path, 'database', str(year)]))

        for month in ymd[year]:
            print(f'\tMonth {month}')

            os.mkdir("/".join([package_path, 'database', str(year), str(month)]))

            mafDataset = monthArticlesFetch(year, month)
            mafDataset.save_to_disk("/".join([package_path, 'database', str(year), str(month)]), num_shards=1, num_proc=min(get_num_proc(), len(mafDataset)))

            if year == current_year and month == current_month:
                file = open("/".join([package_path, 'database', str(year), str(month), 'INCOMPLETE']), 'w')
                file.write('This portion of the dataset was not done being appended at the time when it was downloaded.')
                file.close()

def db_update():

    print('Fetching list of available years and months from the sitemap...')

    ymd = ymdFetch()

    print('Finished fetching years and months.')

    local_ymd = {}

    for year in os.listdir("/".join([package_path, 'database'])):
        local_ymd[int(year)] = []
        for month in os.listdir("/".join([package_path, 'database', year])):
            if "INCOMPLETE" not in os.listdir("/".join([package_path, 'database', year, month])): # don't consider INCOMPLETE ones as already downloaded since they must be re-downloaded
                local_ymd[int(year)].append(int(month))
            else:
                pass
    
    ymd_needs_download = {}

    for year in ymd.keys():
        if year not in local_ymd.keys():
            local_ymd[year] = []
        ymd_needs_download[year] = [x for x in ymd[year] if x not in local_ymd[year]]

    current_year, current_month = get_current_ym()

    for year in ymd_needs_download.keys():
        print(f'Getting Data for {year}')

        if not os.path.exists("/".join([package_path, 'database', str(year)])):
            os.mkdir("/".join([package_path, 'database', str(year)]))

        for month in ymd_needs_download[year]:
            print(f'\tMonth {month}')

            if os.path.exists("/".join([package_path, 'database', str(year), str(month)])):
                shutil.rmtree("/".join([package_path, 'database', str(year), str(month)]))
            os.mkdir("/".join([package_path, 'database', str(year), str(month)]))

            mafDataset = monthArticlesFetch(year, month)
            mafDataset.save_to_disk("/".join([package_path, 'database', str(year), str(month)]), num_shards=1, num_proc=min(get_num_proc(), len(mafDataset)))

            if year == current_year and month == current_month:
                file = open("/".join([package_path, 'database', str(year), str(month), 'INCOMPLETE']), 'w')
                file.write('This portion of the dataset was not done being appended at the time when it was downloaded.')
                file.close()

def db_fetch():
    
    local_files = {}

    for year in os.listdir("/".join([package_path, 'database'])):
        local_files[int(year)] = {}
        for month in os.listdir("/".join([package_path, 'database', year])):
            data_files = [x for x in os.listdir("/".join([package_path, 'database', year, month])) if x.endswith('.arrow')]
            local_files[int(year)][int(month)] = data_files

    return local_files