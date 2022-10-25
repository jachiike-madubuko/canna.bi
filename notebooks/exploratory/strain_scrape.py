from re import L
import pandas as pd 
import requests
import numpy as np
import time
import math
from multiprocessing import Pool
import itertools
# https://curlconverter.com/

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


def get_num_batches():
    url = 'https://consumer-api.leafly.com/api/strain_playlists/v2?&skip=0&take=60'
    response = requests.get(url)
    return math.ceil(response.json()['totalCount']/60)

get_batch = lambda x: f'https://consumer-api.leafly.com/api/strain_playlists/v2?&skip={x*60}&take=60&lat=45.6684&lon=-111.2422'
def get_strain_batch(batch_num):
    try:
        data = requests.get(get_batch(batch_num), headers=headers).json()
        return data['hits']['strain']
    except Exception as e:
        print(f'failed to grab 60 at batch {batch_num}')
        print(e)

if __name__ == '__main__':
    p = Pool()
    records = p.map(get_strain_batch, range(get_num_batches()))
    data = []
    #unnest a list of lists https://stackoverflow.com/a/953097
    # [data.extend(iter(batch)) for batch in records]
    [ent for sublist in records for ent in sublist]
    # https://stackoverflow.com/a/41801708
    strains_df = pd.concat([pd.json_normalize(v, sep='_') for v in data])
    strains_df.reset_index(drop=True, inplace=True)
    strains_df.to_csv('./data/raw/strains.csv',index=False)
