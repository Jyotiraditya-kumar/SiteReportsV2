import requests
import pandas as pd
from src import utilities as utils
import random
from retry import retry
from src import config
from pathlib import Path
from src import clogger as logger
import hashlib
import time
import json
from datetime import datetime
import re

from concurrent.futures import ThreadPoolExecutor

# BASE_DIR = config.PROJECT_DIR / "data" / "zomato"
CACHE_DIR = Path("/media/jyotiraditya/Ultra Touch/Spatic/cache_home/cache_zomato_reviews")
log = logger.get_logger('ZomatoRestaurants')
logging = log


def get_random_proxy(index=None):
    available_proxies = utils.get_my_proxies()
    if len(available_proxies) == 0:
        raise Exception('No proxy available')

    if index is not None and 0 <= index < len(available_proxies):
        index = index % len(available_proxies)
        current_proxy = available_proxies[index]
    else:
        current_proxy_ind = random.choice(range(len(available_proxies)))
        current_proxy = available_proxies[current_proxy_ind]
    proxy = {
        "https": current_proxy
    }
    return proxy


class EmptyPageError(Exception):
    pass


@retry(exceptions=(EmptyPageError,), tries=3, delay=3)
def get_html_response_google(hex_id, num_reviews, proxy):
    # proxy=get_random_proxy()
    BASE_DIR = config.PROJECT_DIR / "data" / "google"
    CACHE_DIR = Path("/media/jyotiraditya/Ultra Touch/Spatic/cache_home/cache_google_reviews_oldest")
    log = logger.get_logger('GoogleReviews')
    logging = log
    dig = hashlib.md5(f"{hex_id}_{num_reviews}_fetch_latest=true".encode()).hexdigest()
    cache_dirs = [BASE_DIR / 'cache', CACHE_DIR, ]
    lambda_url = random.choice([
        "https://u7ub4ix7quehlb4mptoucjvndu0brrnf.lambda-url.us-east-2.on.aws",
        "https://sadxe5vx7h3wdbi3j5z5za3o5a0mxgtk.lambda-url.us-east-1.on.aws"
    ])
    url = f"{lambda_url}/?hex_id={hex_id}&num_reviews={num_reviews}&fetch_latest=true"
    for cache_dir in cache_dirs:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f'{dig}.txt'
        if cache_file.exists():
            with open(cache_file) as f:
                resp = f.read()
                if "Expecting value" in resp:
                    print('deleting file')
                    cache_file.unlink()
                    continue
                return resp
    try:
        print("making request")
        response = requests.get(
            url,
            headers=None,
            proxies=None, timeout=10,
        )
        if response.status_code == 200:
            time.sleep(1)
            text = response.text
            if "Expecting value" in text:
                raise EmptyPageError
            with open(cache_file, 'w+') as f:
                f.write(text)
            data = json.loads(text)
            if data['statusCode'] == 200:
                return text
            else:
                log.error(text)
                raise ValueError
        else:
            log.error(response.text, response.status_code)
            raise EmptyPageError
    except Exception as e:
        log.error(f'{proxy} | {e}')
        raise e


def get_google_reviews(hex_id, num_reviews):
    # print(f"querying : {hex_id = } ")
    if num_reviews < 11:
        return
    # if num_reviews > 0 and num_reviews % 10 == 0:
    #     num_reviews -= 1
    try:
        data = get_html_response_google(hex_id, num_reviews=num_reviews, proxy=None)
    except:
        return None
    if 'Expecting' in data:
        raise ValueError
    else:
        pass
    data = json.loads(data)
    if data['statusCode'] != 200:
        print(data)
        return 0
    # resp = dict(
    #     date=data['data']['date'], number_of_reviews=data['data']['number_of_reviews'])
    return data['data']


def get_reviews_per_day_(cid, number_of_votes, source):
    if number_of_votes < 20 and source == 'gmaps_v2':
        return {'date': 0, "number_of_reviews": 0}
    f = {'gmaps_v2': get_google_reviews, 'zomato': get_reviews_zomato}
    try:
        res = f[source](cid, number_of_votes)
    except KeyError:
        res = {'date': 0, "number_of_reviews": 0}
    return res


def get_reviews_per_day(df, threshold):
    for i in range(10):
        dd = df.to_dict(orient='records')
        cids = [i['cid'] for i in dd]
        reviews = [i['number_of_votes'] for i in dd]
        sources = [i['source'] for i in dd]
        with ThreadPoolExecutor(max_workers=10) as executor:
            dd_ = executor.map(get_reviews_per_day_, cids, reviews, sources)
    df['additional_data'] = list(dd_)
    df = pd.concat([df, pd.json_normalize(df['additional_data'])], axis=1).drop(
        columns="additional_data")
    print(df.columns)
    df['reviews_per_day'] = 86400 * df['number_of_votes'] / (df['created_at'] - df['date'])
    mask = df['source'] == 'zomato'
    df.loc[mask, 'reviews_per_day'] = df.loc[mask]['reviews_per_day'] / 2
    df = df.sort_values(by='reviews_per_day', ascending=False)
    return df


@retry(exceptions=(EmptyPageError,), tries=3, delay=1)
def get_html_response(url, proxy):
    proxy = get_random_proxy()
    # proxy=None
    BASE_DIR = config.PROJECT_DIR / "data" / "zomato"

    dig = hashlib.md5(url.encode()).hexdigest()
    cache_dirs = [BASE_DIR / 'cache', CACHE_DIR, ]
    for cache_dir in cache_dirs:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file = cache_dir / f'{dig}.txt'
        if cache_file.exists():
            with open(cache_file) as f:
                resp = f.read()
                return resp
    # headers = get_headers()
    headers = {"User-Agent": "PostmanRuntime/7.32.3",
               "Accept": "*/*",
               "Cache-Control": "no-cache",
               "Postman-Token": "51227458-1214-44a6-8f4b-f761ec0099ce",
               "Host": "www.zomato.com",
               "Accept-Encoding": "gzip, deflate, br",
               "Connection": "keep-alive",
               "Cookie": r"PHPSESSID=d211e97ee90f64010ad7b769d4d04ecf; ak_bmsc=4B18F182E4143B6397D227D52152B8BB~000000000000000000000000000000~YAAQbW4/F7lGi0KKAQAAw6m+ShSNTUWoLjzwHd3EHr5WBV5Knrg6PnMYUNEMXodRwWzhxg6ngrqA00b8kNSNTmwUnJn0m+6GqjEK/lCRk/1SRHCTcMSdSiBk2JgAqWYYDekRP05CDc20hjCwhzA1/jgF9NE2t1zShtAEnOVc2zgjGa4bWLEnhQxu/GNs1B6YYs9GomL6oq7tXBDd2ntUtWWCG/G8EKN3YH3VUnTB5Glc77VFkJtMLgX2dO8Vi+NT8x194KT0hRbdTH8rXLUo2O6qQjSiHRxSKoIv60oLeu52ch9Bm3VDiHMhWSRrL02BGipXnZkidWjwLpmx51j6FgBgrnW0qoQklpVhQ1lTsT/oFssWZChORJwRq4s=; csrf=21d1af5ae835429f207597a6cec658d6; fbcity=3; fbtrack=eeb9eaaeb90b494e2b326e56643a9bad; fre=0; rd=1380000; zl=en; AWSALBTG=RK0ccuPffBh8VFY/BolIJO3spzo2UtOXUYpj1ruvnVmsCj/pwymk0pgOYRb1U2QtgR2mhV2DYWDmeuqOVVMTRhpSS0+TcPNYh1IElZN9vXVqZ/9G1isrxtMou4ghKKSzdD6Yx8zwzx9Kpjb+Vhe0gDFvXnubSa+oliwYNbKfhlQg; AWSALBTGCORS=RK0ccuPffBh8VFY/BolIJO3spzo2UtOXUYpj1ruvnVmsCj/pwymk0pgOYRb1U2QtgR2mhV2DYWDmeuqOVVMTRhpSS0+TcPNYh1IElZN9vXVqZ/9G1isrxtMou4ghKKSzdD6Yx8zwzx9Kpjb+Vhe0gDFvXnubSa+oliwYNbKfhlQg; locus=%7B%22addressId%22%3A0%2C%22lat%22%3A19.017656%2C%22lng%22%3A72.856178%2C%22cityId%22%3A3%2C%22ltv%22%3A3%2C%22lty%22%3A%22city%22%2C%22fetchFromGoogle%22%3Afalse%2C%22dszId%22%3A77482%2C%22fen%22%3A%22Mumbai%22%7D; ltv=3; lty=3"
               }

    try:
        response = requests.get(
            url,
            headers=headers,
            proxies=proxy, timeout=10,
        )
        if response.status_code == 200:
            with open(cache_file, 'w+') as f:
                f.write(response.text)
            return response.text
        else:
            log.error(response.text, response.status_code)
            raise EmptyPageError
    except Exception as e:
        log.error(f'{proxy} | {e}')
        raise EmptyPageError


def get_reviews_zomato(rest_id, number_of_reviews):
    url = 'https://www.zomato.com/webroutes/reviews/loadMore?res_id={rest_id}&filter=reviews-dd&sort=da'
    url_ = url.format(rest_id=rest_id)
    # print(f"querying : {url_}")
    data = get_html_response(url_, None)
    data = json.loads(data)
    reviews = data['entities']["REVIEWS"]
    timestamps = [reviews[k]['timestamp'] for k in reviews]
    if not timestamps:
        return None
    date = timestamps[0]
    s = data['page_data']['sections']['SECTION_REVIEWS']['pageReviewsText']
    pattern = re.compile("of (?P<number_of_reviews>\d+) reviews")
    try:
        res = pattern.search(s).groupdict()
    except:
        res = {}
    try:
        date = datetime.strptime(date, "%b %d, %Y").timestamp()
        res['date'] = int(date)
    except:
        res['date']=0
    return res


# if __name__ == '__main__':
#     project_ids = ['130110_775547', '130009_776325', '130552_777638']
#     project_ids += ['130631_776205', '128873_775969']
# 
#     for project_id in project_ids[:]:
#         threshold = 30
#         df = pd.read_csv(f"data/projects_data/{project_id}/raw/location_poi_data.csv")
#         df_ = df[
#             (df['number_of_votes'] > 0) & ((
#                 df['category'].isin(['college', 'hospital', 'clinic', 'pharmacy'])) | ((df['category'] == 'school') & df[
#                 'type'].isin(["pre_school", 'high_school', 'play_school', 'N_A'])))]
#         df_.to_csv(f"data/projects_data/{project_id}/demand_generator_data.csv", index=False)
#         mask = (df['number_of_votes'] > 0) & (df['category'].isin(
#             ['clothing_store', 'shoe_store', 'restaurant', 'home_decor', 'jewelry_store', 'coffee_shop', 'supermarket',
#              'electronic_store', 'cosmetic']))
# 
#         df = df[mask].reset_index(drop=True)
#         # df['reviews_per_day'] = df['number_of_votes']
#         # df = df.sort_values('reviews_per_day', ascending=False)
#         print(df.shape)
#         df = get_reviews_per_day(df, threshold=threshold)
#         df.to_csv(f"data/projects_data/{project_id}/poi_data.csv", index=False)

if __name__ == '__main__':
    project_ids = ['130631_776205', '128873_775969']
    for project_id in project_ids[:]:
        threshold = 30
        df = pd.read_csv(f"data/projects_data/{project_id}/competitors.csv")
        print(df.shape)
        df = get_reviews_per_day(df, threshold=threshold)
        # df['reviews_per_day'] = df['number_of_votes']
        df = df.sort_values('reviews_per_day', ascending=False)
        df.to_csv(f"data/projects_data/{project_id}/competitors_ranked.csv", index=False)
