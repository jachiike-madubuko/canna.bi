import pandas as pd 
import requests
import numpy as np
import time
import math
from multiprocessing import Pool

# https://github.com/saiskee/Leafly-scraper
# https://curlconverter.com/
headers = {
    'authority': 'consumer-api.leafly.com',
    'accept': 'application/json',
    'accept-language': 'en-GB,en;q=0.7',
    # Requests sorts cookies= alphabetically
    'origin': 'https://www.leafly.com',
    'referer': 'https://www.leafly.com/',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'sec-gpc': '1',
    'user-agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1',
    'x-app': 'web-web',
    'x-environment': 'prod',
}

review_request = lambda strain, skip, take: f'https://consumer-api.leafly.com/api/strains/v1/{strain}/reviews?skip={skip*60}&take={take}&sort\\[0\\]\\[upvotes_count\\]=desc'
get_review_num = lambda strain: requests.get(review_request(strain, 0, 0 ), headers=headers).json()['metadata']['totalCount']
# get_review_batch = lambda strain, skip: requests.get(review_request(strain, skip, 60 ), headers=headers).json()['data']

def get_review_batch( strain, skip): 
    resp = requests.get(review_request(strain, skip, 60 ), headers=headers).json()
    try:
        return resp['data']
    except Exception:
        print(resp)

completed_strains = []
def get_strain_reviews(strain):
    if strain in completed_strains:
        return False
    review_data = []
    num_reviews_batches = math.ceil(get_review_num(strain)/60)
    for i in range(num_reviews_batches):
        time.sleep(.5)
        if i%20==0:
            print(f'{strain} : {i}/{num_reviews_batches}')
        try:
            data = get_review_batch(strain, i)        
            review_data.extend(iter(data))
        except Exception as e:
            print(f'failed to grab 60 at {60*i}')
            print(e)
    # https://stackoverflow.com/a/41801708
    reviews_df = pd.concat([pd.json_normalize(v, sep='_') for v in review_data])
    reviews_df.reset_index(drop=True, inplace=True)
    reviews_df.to_csv(f'./data/raw/reviews/{strain}_reviews.csv')
    completed_strains.append(strain)
    
strains = pd.read_csv('./data/raw/strains.csv')


    
if __name__ == '__main__':
    p = Pool()
    records = p.map(get_strain_reviews, strains.slug)
    
    
    #TODO https://medium.com/@lminhkhoa/how-to-build-a-fully-automated-web-scraping-pipeline-for-dashboard-742b6dce9f0f