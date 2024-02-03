import numpy as np
import requests
from typing import Literal
import pandas as pd
import shapely

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
        yield df[i:i + chunk_size]


def get_free_api_key():
    url = ''' https://www.andindia.com/on/demandware.store/Sites-AND-Site/default/Stores-FindStores?showMap=false&radius=30&lat=28.7040592&long=77.10249019999999 '''
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json(strict=False)
        key_url = data['googleMapsApi']
        key = key_url.split('key=')[-1]
        return key
    else:
        print(response.status_code)
        return None
    
def get_parquet_files_columns_info(file):
    import subprocess
    import json
    output=subprocess.getoutput(f'parquet-tools inspect "{file}" --detail')
    cols=json.loads(output.split('org.apache.spark.sql.parquet.row.metadata')[1].strip().split('value =')[1].strip().split('created_by =')[0].strip())
    return cols['fields']

def get_city_polygon(city:Literal['bangalore','pune','ncr','mumbai','chennai','hyderabad','kolkata','ahmedabad','jaipur','surat']):
    df=pd.read_csv('/home/jyotiraditya/PycharmProjects/jupyterProject/notebooks/data/top_10_cities_2d.csv')
    poly=df[df['Name']==city].to_dict(orient='records')[0]['geometry']
    poly=shapely.from_wkt(poly)

    return poly

if __name__ == "__main__":
    pol=get_city_polygon('pune')
    print(pol)
