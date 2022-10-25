
import pandas as pd 
import requests
import numpy as np
import time
import math
from multiprocessing import Pool
from bs4 import BeautifulSoup
import time


headers = {
    'authority': 'consumer-api.leafly.com',
    'accept': 'application/json',
    'accept-language': 'en-GB,en;q=0.6',
    # Requests sorts cookies= alphabetically
    'if-none-match': 'W/"224a3f637e080602bd933b9d99864385"',
    'origin': 'https://www.leafly.com',
    'referer': 'https://www.leafly.com/',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'sec-gpc': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.5112.102 Safari/537.36',
    'x-app': 'web-web',
    'x-environment': 'prod',
}

def find_parents(strain):
    time.sleep(2)
    page= requests.get(f'https://www.leafly.com/strains/{strain}', headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    if soup.find('div' , class_="lineage__strain--two-parents") :
        try: 
            left = soup.find('div', class_='lineage__left-parent')
            right = soup.find('div', class_='lineage__right-parent')
            parent1 = left.find('a')['href'].split('/')[-1]
            parent2 = right.find('a')['href'].split('/')[-1]
            return (strain, parent1, parent2)
            # print(f'{strain}: {parent1}<=>{parent2}')
        except Exception as e:
            print(e)
            return (strain,None,None)
    elif soup.find('div' , class_="lineage__strain--single-parent") :
        try: 
            center = soup.find('div', class_='lineage__center-parent')
            parent = center.find('a')['href'].split('/')[-1]
            return (strain, parent, None)
            # print(f'{strain}: {parent1}<=>{parent2}')
        except Exception as e:
            print(e)
            return (strain,None,None)
    elif soup.find('div' , class_="lineage__strain--no-parents") :    
            return (strain,None,None)
    
    
    
if __name__ == '__main__':
    p = Pool()
    strains = pd.read_csv('./data/raw/strains.csv')
    records = p.map(find_parents, strains.slug[:50])
    pd.DataFrame.from_records(records, columns=['slug', 'parent_1', 'parent_2']).to_csv('./data/raw/linage.csv')