import numpy as np
import requests
from typing import Literal
import pandas as pd
import shapely
import geopandas as gpd
import requests
from bs4 import BeautifulSoup
import json
from retry import retry
import pathlib
from hashlib import md5
from curl_cffi import requests as requests2


class EmptyPageError(Exception):
    pass


@retry((EmptyPageError, TimeoutError), tries=1, delay=30)
def cached_request(
    url, method="GET", cache_folder=".", headers=None, proxy=None, timeout=10, data=None
):
    common_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    }
    if headers is None:
        headers = {}
    current_proxy = proxy
    headers = {**common_headers, **headers}
    url_md5 = md5(url.encode()).hexdigest()
    p = pathlib.Path(f"{cache_folder}/{url_md5}.html")

    p.parent.mkdir(exist_ok=True, parents=True)
    if p.exists():
        with open(p, "r") as file:
            data = file.read()
    else:
        try:
            resp = requests.request(
                method=method,
                url=url,
                proxies=current_proxy,
                timeout=timeout,
                headers=headers,
                data=data,
                verify=False,
            )
            if resp.status_code != 200:
                print(
                    f"Error ==> Captcha Error | errorcode : {resp.status_code} | {resp.text}"
                )
                raise EmptyPageError
        except Exception as e:
            # available_proxies.pop(proxy_ind)
            print(f"{e} ")
            raise EmptyPageError
        data = resp.text
        with open(p, "w+") as file:
            file.write(data)
    return data


@retry((EmptyPageError, TimeoutError), tries=6, delay=1)
def cached_request2(
    url,
    method="GET",
    cache_folder=".",
    headers=None,
    proxy=None,
    timeout=10,
    impersonate="chrome101",
    verify=False,
):
    common_headers = {
        # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        # "Accept-Encoding": "gzip, deflate, br",
        # "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    }
    # common_headers={}
    if headers is None:
        headers = {}
    current_proxy = proxy
    headers = {**common_headers, **headers}
    url_md5 = md5(url.encode()).hexdigest()
    p = pathlib.Path(f"{cache_folder}/{url_md5}.html")
    # print(url)
    p.parent.mkdir(exist_ok=True, parents=True)
    if p.exists():
        with open(p, "r") as file:
            data = file.read()
    else:
        try:
            resp = requests2.request(
                method=method,
                url=url,
                proxies=current_proxy,
                timeout=timeout,
                headers=headers,
                impersonate=impersonate,
                verify=False,
            )
            if resp.status_code != 200:
                print(
                    f"Error ==> Captcha Error | errorcode : {resp.status_code} | {resp.text}"
                )
                raise EmptyPageError
        except Exception as e:
            # available_proxies.pop(proxy_ind)
            print(f"{e} ")
            raise EmptyPageError
        data = resp.text
        with open(p, "w+") as file:
            file.write(data)
    return data


def get_url_from_cache(url, cache_folder=".", slug=""):
    url_md5 = md5(f"{url}{slug}".encode()).hexdigest()
    p = pathlib.Path(f"{cache_folder}/{url_md5}.html")

    p.parent.mkdir(exist_ok=True, parents=True)
    if p.exists():
        with open(p) as file:
            data = file.read()
        return data
    else:
        raise ValueError("Cache Not Found")


def delete_cache(url, cache_folder=".", slug=""):
    url_md5 = md5(f"{url}{slug}".encode()).hexdigest()
    p = pathlib.Path(f"{cache_folder}/{url_md5}.html")

    p.parent.mkdir(exist_ok=True, parents=True)
    if p.exists():
        p.unlink()
        return 1
    else:
        return 0


@retry((EmptyPageError, TimeoutError), tries=1, delay=30)
def cached_post_request(
    url, cache_folder=".", headers=None, proxy=None, timeout=10, data=None, slug=""
):
    common_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    }
    if headers is None:
        headers = {}
    current_proxy = proxy
    headers = {**common_headers, **headers}

    url_md5 = md5(f"{url}{slug}".encode()).hexdigest()
    p = pathlib.Path(f"{cache_folder}/{url_md5}.html")

    p.parent.mkdir(exist_ok=True, parents=True)
    if p.exists():
        with open(p, "r") as file:
            data = file.read()
    else:
        try:
            resp = requests.request(
                method="POST",
                url=url,
                proxies=current_proxy,
                timeout=timeout,
                headers=headers,
                data=data,
            )
            if resp.status_code != 200:
                print(
                    f"Error ==> Captcha Error | errorcode : {resp.status_code} | {resp.text}"
                )
                raise EmptyPageError
        except Exception as e:
            # available_proxies.pop(proxy_ind)
            print(f"{e} ")
            raise EmptyPageError
        data = resp.text
        with open(p, "w+") as file:
            file.write(data)
    return data


def get_my_proxies():
    all_proxies = []
    users = [
        # {"username":"irgtsnqy","password":"3izt7b9mbl82"},
        # {"username":"opscsswa","password":"k4k98eqjzcet"},
        # {"username":"ybhdmgyw","password":"7biqvmuda8c3"},
        # {"username":"vazswedv","password":"gp2p7eoo3v5k"},
        # {"username":"qqxdxdbt","password":"gmttetzzd6wf"},
        # {"username":"egvcltxn","password":"x57dyd9ozcdn"},
        {"username": "lkhbnpeu", "password": "pidnftflj6mr"},
        {"username": "ccxmzaxq", "password": "jce571t4huyp"},
        {"username": "ipantjyx", "password": "5quoonalg34x"},
        # {"username":"bs
        # muhn","password":"qltt2wfdgqlq"},
        {"username": "tuphkhls", "password": "pkq1p7r3chqv"},
        {"username": "yrtalzvi", "password": "jwvc7u8st6e3"},
        {"username": "ajgztszz", "password": "dgsw734nwrt6"},
        {"username": "kgfiucel", "password": "aa8kpl6nbaia"},
        {"username": "wdaicwtz", "password": "1dcqw01x2upz"},
        {"username": "cnjttued", "password": "weh96h4kmpn5"},
        {"username": "ounwxgaa", "password": "75ew9d01vvme"},
        {"username": "ayxdzmub", "password": "ru4v0b69kkv3"},
        {"username": "fsdplxwj", "password": "69n6egr0jo5w"},
        {"username": "klkcvoml", "password": "kov76w2e2kzq"},
        {"username": "pmcpgxud", "password": "gsbr0pyafo9b"},
        {"username": "iuqvpqzf", "password": "5iqjg9ybooit"},
        {"username": "nhbilqnb", "password": "ax0qmf3rc18a"},
        {"username": "vrfvzmjc", "password": "1s863hnoy7vn"},
        {"username": "kderjtpq", "password": "xlopjzs4insy"},
    ]
    proxies = [
        "http://{username}:{password}@38.154.227.167:5868",
        "http://{username}:{password}@185.199.229.156:7492",
        "http://{username}:{password}@185.199.228.220:7300",
        "http://{username}:{password}@185.199.231.45:8382",
        "http://{username}:{password}@188.74.210.207:6286",
        "http://{username}:{password}@188.74.183.10:8279",
        "http://{username}:{password}@188.74.210.21:6100",
        "http://{username}:{password}@45.155.68.129:8133",
        "http://{username}:{password}@154.95.36.199:6893",
        "http://{username}:{password}@45.94.47.66:8110",
    ]
    for user in users:
        all_proxies.extend([proxy.format(**user) for proxy in proxies])
    return all_proxies


def get_other_proxies():
    proxies = [
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10000",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10001",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10002",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10003",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10004",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10005",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10006",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10007",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10008",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10009",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10030",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10031",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10032",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10033",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10034",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10064",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10065",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10066",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10067",
        "http://vsatyen:unWLWiVPCd@107.181.187.120:10068",
    ]
    return proxies

def check_proxy(proxy):
    import requests
    headers={'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7', 'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7', 'Cache-Control': 'no-cache', 'Dnt': '1', 'Pragma': 'no-cache', 'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"', 'Sec-Ch-Ua-Mobile': '?0', 'Sec-Ch-Ua-Platform': '"Linux"', 'Sec-Fetch-Dest': 'document', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Site': 'none', 'Sec-Fetch-User': '?1', 'Upgrade-Insecure-Requests': '1', 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
    url = "https://httpbin.org/ip"
    # print(proxy)
    try:
        resp = requests.get(url=url, proxies={"https": proxy},headers=headers)
        print(proxy, resp.status_code, resp.text)
    except Exception as e:
        print(proxy, e)




def check_all_proxies():
    proxies=get_my_proxies()[::10] #+ get_other_proxies()
    print(proxies)
    # proxies = [proxies[i] for i in range(0, len(proxies), 1)]
    for proxy in proxies:
        res = check_proxy(proxy=proxy)


def flatten_list(lst: list):
    result = []
    for i in lst:
        if isinstance(i, list):
            result.extend(flatten_list(i))
        else:
            result.append(i)
    return result


def split_df_chunk_count(df, chunks_count):
    df_split = np.array_split(df, chunks_count)
    return df_split


def split_df_chunk_size(df, chunk_size):
    n = len(df)
    for i in range(0, n, chunk_size):
        yield df[i : i + chunk_size]


def get_free_api_key():
    url = """ https://www.andindia.com/on/demandware.store/Sites-AND-Site/default/Stores-FindStores?showMap=false&radius=30&lat=28.7040592&long=77.10249019999999 """
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json(strict=False)
        key_url = data["googleMapsApi"]
        key = key_url.split("key=")[-1]
        return key
    else:
        print(response.status_code)
        return None


def get_parquet_files_columns_info(file):
    import subprocess
    import json

    output = subprocess.getoutput(f'parquet-tools inspect "{file}" --detail')
    cols = json.loads(
        output.split("org.apache.spark.sql.parquet.row.metadata")[1]
        .strip()
        .split("value =")[1]
        .strip()
        .split("created_by =")[0]
        .strip()
    )
    return cols["fields"]


def get_city_polygon(
    city: Literal[
        "bangalore",
        "pune",
        "ncr",
        "mumbai",
        "chennai",
        "hyderabad",
        "kolkata",
        "ahmedabad",
        "jaipur",
        "surat",
    ]
):
    df = pd.read_csv(
        "/home/jyotiraditya/PycharmProjects/jupyterProject/notebooks/data/top_10_cities_2d.csv"
    )
    poly = df[df["Name"] == city].to_dict(orient="records")[0]["geometry"]
    poly = shapely.from_wkt(poly)
    return poly


def read_geopandas_file(filename, geometry_column="geometry"):
    df = gpd.read_file(
        filename, GEOM_POSSIBLE_NAMES=geometry_column, KEEP_GEOM_COLUMNS="NO"
    )
    return df


def read_geopandas_parquet_file(filename, geometry_column="geometry"):
    df = gpd.read_parquet(filename)
    return df


def format_proxies(file):
    def _format_proxy(p):
        proxy = f'http://{p["user"]}:{p["password"]}@{p["ip"]}:{p["port"]}'
        return proxy

    df = pd.read_csv(
        file, delimiter=";", header=None, names=["ip", "port", "user", "password"]
    )
    proxies = df.to_dict(orient="records")
    proxies = list(map(_format_proxy, proxies))
    with open("proxies.json", "w") as f:
        json.dump(proxies, f, indent=2)

    print(proxies)
    return proxies


if __name__ == "__main__":
    check_all_proxies()
    # format_proxies("/home/jyotiraditya/Downloads/list_proxyseller_jan 2024.csv")
