import json

import geopandas as gpd
import pandas as pd
import numpy as np
from geopy.distance import geodesic
import shapely
from pathlib import Path
import utils as U
import data_extraction as D



def get_distance_from_site(df, lat, lng):
    if df.shape[0] == 0:
        return df.assign(distance=None)
    df['distance'] = df[['lat', 'lng']].apply(lambda x: round(geodesic((x[0], x[1]), (lat, lng)).km, 2), axis=1)
    return df


def get_avg_cft(poly, lat, lng,*args,**kwargs):
    avg_cft = D.DataExtraction(poly).get_avg_cft()
    return avg_cft


def get_intersection_ratio(g1, g2):
    if not g2:
        return 0
    intersecting_area = (g1.intersection(g2).area / g2.area)
    return intersecting_area


# @cool_decorator
def get_population_index(poly, lat, lng,*args,**kwargs):
    pop = D.DataExtraction(poly).get_location_population_data()
    # return pop.to_dict('records')[0]
    pop.fillna(0, inplace=True)

    cols = ["total_population", "age_0_19", "age_20_34", "age_35_59", "age_60+"]
    pop_gdf = gpd.GeoDataFrame(pop, geometry=gpd.GeoSeries.from_wkt(pop['geometry']))
    pop_gdf['pop_grid_geometry'] = pop_gdf['geometry']
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined = gpd.sjoin(stores_gdf, pop_gdf, how="left", predicate='intersects')
    joined["intersection_ratio"] = joined[['geometry', 'pop_grid_geometry']].apply(
        lambda x: get_intersection_ratio(x[0], x[1]), axis=1)
    for col in cols:
        joined[col] = joined[col] * joined['intersection_ratio']
    fns = {col: (col, np.sum) for col in cols}
    age_group = joined.groupby("store_name").agg(**fns).to_dict('records')[0]
    return age_group


# @cool_decorator
def get_companies(poly, lat, lng,*args,**kwargs):
    joined = D.DataExtraction(poly).get_companies_data().set_index('type')
    joined = joined.T.reset_index(drop=True)
    for c in ["India's 501-1000", "India's Top 500"]:
        if c not in joined.columns.tolist():
            joined[c] = 0
    try:
        joined["India's Top 1000"] = joined[["India's 501-1000", "India's Top 500"]].sum(axis=1)
    except:
        joined["India's Top 1000"] = 0
    joined['company_count'] = joined.sum(axis=1)
    if joined.shape[0] == 0:
        return {}
    return joined.to_dict('records')[0]


# @cool_decorator
def get_demand_generator_data(poly, lat, lng,*args,**kwargs):
    joined = D.DataExtraction(poly).get_demand_generators()
    return dict((joined[['category', 'count']].values.tolist()))


# @cool_decorator
def get_projects(poly, lat, lng,*args,**kwargs):
    projects = D.DataExtraction(poly).get_location_projects_data()
    projects['default_units'] = projects['final_num_units'].apply(lambda x: 40 if x == 0.0 else x)

    joined_ = get_distance_from_site(projects, lat, lng)
    aggs = {
        f"num_units": ("final_num_units", sum),
        f"default_units": ("default_units", sum),
        f"count_projects": ("id", np.count_nonzero),
    }
    joined_['store_name'] = 1
    joined = joined_.groupby("store_name").agg(**aggs).reset_index()
    joined = joined.drop(columns=["store_name"])
    if joined.shape[0] > 0:
        resp = joined.to_dict(orient="records")[0]
    else:
        resp = dict(zip(['num_units', 'default_units', 'count_projects'], [0, 0, 0]))
    resp['projects'] = joined_[
        ['id', "name", 'lat', 'lng', 'final_num_units', 'default_units', 'distance']].sort_values(
        by='final_num_units', ascending=False).fillna(0).to_dict(orient="records")
    return resp


# @cool_decorator
def get_income_distribution(poly, lat, lng,*args,**kwargs):
    aff_ind = D.DataExtraction(poly).get_location_income_distribution_data()
    aff_ind_gdf = gpd.GeoDataFrame(aff_ind, geometry=gpd.GeoSeries.from_wkt(aff_ind['geom']))
    aff_ind_gdf['geom'] = aff_ind_gdf['geometry']  # .apply(shapely.from_wkt)
    aff_ind_gdf.columns = ['EWS_HH', 'LIG_HH', 'MIG_HH', 'AIG_HH', 'EIG_HH', 'avg_household_income', 'geom', 'geometry']
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined = gpd.sjoin(stores_gdf, aff_ind_gdf, how="left", predicate='intersects')
    cols = ['EWS_HH', 'LIG_HH', 'MIG_HH', 'AIG_HH', 'EIG_HH', ]
    joined['intersection_ratio'] = joined[['geometry', 'geom']].apply(
        lambda x: (get_intersection_ratio(x[0], x[1])), axis=1)
    for col in cols:
        joined[col] = (joined[col] * joined['intersection_ratio']).astype(int)
    joined['total_hh'] = joined[cols].sum(axis=1)
    joined["middle+affluent"] = joined[['MIG_HH', 'AIG_HH', 'EIG_HH']].sum(axis=1)
    cols = ['EWS_HH', 'LIG_HH', 'MIG_HH', 'AIG_HH', 'EIG_HH', "middle+affluent", 'total_hh']
    fns = {col: (col, np.sum) for col in cols}
    fns['median_household_income'] = ("avg_household_income", np.median)
    hh_distribution = joined.groupby("store_name").agg(**fns).to_dict('records')[0]
    return hh_distribution


# @cool_decorator
def get_competition(poly, lat, lng,*args,**kwargs):
    malls = D.DataExtraction(poly).get_competitors_data(**kwargs)
    malls = gpd.GeoDataFrame(malls, geometry=gpd.points_from_xy(malls['lng'], malls['lat']))
    joined = malls.sort_values(by=['reviews_per_day'], ascending=False)
    joined = joined[['id', 'name', 'number_of_votes', 'rating', 'lat', 'lng', 'reviews_per_day', 'competitor_type']]
    if joined.shape[0] > 0:
        joined = get_distance_from_site(joined, lat,lng)
        joined = joined[joined['distance'] > 0].reset_index(drop=True)
    joined['type'] = joined['competitor_type']
    resp = {"count": joined.shape[0], 'pois': joined.to_dict('records')}
    return resp


# @cool_decorator
def get_malls(poly, lat, lng,*args,**kwargs):
    joined = D.DataExtraction(poly).get_malls()
    joined = joined.sort_values(by=['reviews_per_day'], ascending=False)
    joined = joined[['id', 'name', 'number_of_votes', 'rating', 'lat', 'lng', 'reviews_per_day']]
    joined = joined.fillna({"reviews_per_day": 0})
    joined = get_distance_from_site(joined, lat, lng)
    if joined.shape[0] > 0:
        joined = joined[joined['distance'] > 0].reset_index(drop=True)
    resp = {"count": joined.shape[0], 'pois': joined.to_dict('records')}
    return resp


# @cool_decorator


# @cool_decorator
def get_category_count(poly, lat, lng,*args,**kwargs):
    df = D.DataExtraction(poly).get_location_poi_data()
    df['brand_name'] = df['brand_name'].str.title()
    df['store_name'] = 1
    joined_ = df.sort_values(by=['reviews_per_day'], ascending=False)
    joined_ = get_distance_from_site(joined_, lat, lng)
    g = joined_[
        ['id', 'name', 'lat', 'lng', 'source', 'brand_id', 'brand_name', 'category', 'number_of_votes',
         'reviews_per_day', 'distance']].groupby(
        ['category'])
    dict_list = []
    for name, group in g:
        d = {'category': name[0], 'top_pois': group.to_dict('records')}
        dict_list.append(d)
    top_pois = pd.DataFrame(dict_list)
    joined1 = joined_.groupby(["store_name", "category"]).agg(
        **{f"count": ("id", np.count_nonzero), "avg_number_of_reviews_per_day": ("reviews_per_day", np.mean)}
    ).reset_index()
    joined1 = joined1.fillna(0)
    joined0 = joined1.groupby(["store_name"]).agg(count=("count", np.sum), avg_number_of_reviews_per_day=(
        "avg_number_of_reviews_per_day", np.mean)).to_dict('records')[0]
    joined2 = joined_[joined_["brand_id"] != 'N_A'].groupby(["store_name", "category", 'brand_name']).agg(
        **{f"ranking": ("reviews_per_day", np.sum),
           }
    ).reset_index().groupby(["category", ])[['brand_name', 'ranking']].apply(
        lambda g: dict(map(tuple, g.values.tolist()))).reset_index().rename(columns={0: 'top_brands'})
    joined = joined1.merge(top_pois, how="left", on='category').merge(joined2, how="left", on='category')
    joined = joined.reset_index(drop=True)
    joined = joined.replace({pd.NA: None})
    joined = joined[['category', 'count', 'avg_number_of_reviews_per_day', 'top_pois', 'top_brands']].to_dict(
        orient='records')
    res = joined0
    res['data'] = joined
    return res


# @cool_decorator
def get_rent_and_sale_prices(poly, lat, lng,*args,**kwargs):
    df = D.DataExtraction(poly).get_location_properties()
    df = df.fillna({"price": 0, 'type': '', 'trans_type': '', 'desc': '', 'name': ''})
    df['trans_type'] = df['buy_rent']
    df['type'] = df['property_type']
    df['trans_type'] = df['trans_type'].str.lower()
    mask = (df['type'] == 'Apartment') & (df['trans_type'].isin(['buy', 'rent']))
    df = df[mask].drop(columns=['property_type', 'buy_rent']).reset_index(drop=True)
    joined_ = df.sort_values(by="price", ascending=False)
    dict_list = []
    joined_ = get_distance_from_site(joined_, lat, lng).fillna((0))
    g = joined_[df.columns.tolist() + ['distance']].groupby('trans_type')
    for name, group in g:
        d = {'trans_type': name, 'top_pois': group.to_dict('records')}
        dict_list.append(d)
    top_pois = pd.DataFrame(dict_list)
    joined_ = joined_.fillna(0)
    joined = joined_.groupby(["trans_type"]).agg(
        **{f"median_price": ("price", np.median),
           f"count": ("id", np.count_nonzero)
           }
    ).reset_index()
    if top_pois.shape[0] <= 0:
        top_pois = top_pois.assign(trans_type=None)
    joined = joined.merge(top_pois, how='left', on='trans_type').fillna(0)

    # res = {}
    # res['price'] = joined
    # res['apartments'] = joined_[df.columns.tolist() + ['distance']].to_dict(orient='records')
    return joined.to_dict(orient='records')


# @cool_decorator
def get_high_streets(poly, lat, lng,*args,**kwargs):
    joined = D.DataExtraction(poly).get_high_streets()
    joined_ = get_distance_from_site(joined, lat, lng)
    res = joined_.to_dict(orient='records')
    return res

# if __name__ == "__main__":
#
