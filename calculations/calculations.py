import geopandas as gpd
import pandas as pd
import numpy as np
from geopy.distance import geodesic


def get_avg_cft(poly):
    restaurants = pd.read_csv("data/zomato_restaurants.csv")
    restaurants_gdf = gpd.GeoDataFrame(restaurants, geometry=gpd.points_from_xy(restaurants['lng'], restaurants['lat']))
    store_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined_ = store_gdf.sjoin(restaurants_gdf, predicate='contains', how='inner')
    avg_cft = np.mean(joined_['cost_for_two'])
    return {"avg_cft": avg_cft}


def get_population_of_intersection(pop, g1, g2):
    if not g2:
        return 0
    intersecting_area = (g1.intersection(g2).area / g2.area)
    return intersecting_area * pop


def get_population_index(poly):
    aff_ind = pd.read_csv("data/100m_grid_population.csv")
    cols = ["total_population", "f_0", "f_1", "f_10", "f_15", "f_20", "f_25", "f_30", "f_35", "f_40", "f_45", "f_5",
            "f_50", "f_55", "f_60", "f_65", "f_70", "f_75", "f_80", "m_0", "m_1", "m_10", "m_15", "m_20", "m_25",
            "m_30", "m_35", "m_40", "m_45", "m_5", "m_50", "m_55", "m_60", "m_65", "m_70", "m_75", "m_80"]
    aff_ind_gdf = gpd.GeoDataFrame(aff_ind,
                                   geometry=gpd.GeoSeries.from_wkt(aff_ind['geometry']))
    aff_ind_gdf['pop_grid_geometry'] = aff_ind_gdf['geometry']
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined = gpd.sjoin(stores_gdf, aff_ind_gdf, how="left", predicate='intersects')
    for col in cols:
        joined[f'new_{col}'] = joined[[col, 'geometry', 'pop_grid_geometry']].apply(
            lambda x: get_population_of_intersection(x[0], x[1], x[2]), axis=1)
    cols = ["total_population", "f_0", "f_1", "f_10", "f_15", "f_20", "f_25", "f_30", "f_35", "f_40", "f_45", "f_5",
            "f_50", "f_55", "f_60", "f_65", "f_70", "f_75", "f_80", "m_0", "m_1", "m_10", "m_15", "m_20", "m_25",
            "m_30", "m_35", "m_40", "m_45", "m_5", "m_50", "m_55", "m_60", "m_65", "m_70", "m_75", "m_80"]
    fns = {col: (col, np.sum) for col in cols}
    age_group = joined.groupby("store_name").agg(**fns).to_dict('records')[0]
    for i in [0, 1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80]:
        age_group[f"p_{i}"] = age_group[f"m_{i}"] + age_group[f"f_{i}"]
    return age_group


def get_households(poly):
    aff_ind = pd.read_csv("data/bng_household_data.csv").fillna({'grid_hh': 0})
    aff_ind_gdf = gpd.GeoDataFrame(aff_ind[["geoid", "grid_hh"]], geometry=gpd.GeoSeries.from_wkt(aff_ind['geometry']))
    aff_ind_gdf['grid_geometry'] = aff_ind_gdf['geometry']
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined = gpd.sjoin(stores_gdf, aff_ind_gdf, how="left", predicate='intersects')
    joined['new_hh'] = joined[['grid_hh', 'geometry', 'grid_geometry']].apply(
        lambda x: get_population_of_intersection(x[0], x[1], x[2]), axis=1)
    aff_ind_city = int(np.sum(joined['new_hh']))
    # print("aff index",aff_ind_city)
    return {'households': aff_ind_city}


def get_companies(poly):
    companies = pd.read_csv('data/company_data.csv')
    companies_gdf = gpd.GeoDataFrame(companies, geometry=gpd.points_from_xy(companies['lng'], companies['lat']))
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    geometry_col = 'geometry'
    joined = stores_gdf.sjoin(companies_gdf, how="inner", predicate="contains")
    joined = joined.groupby(["store_name", "type"]).agg(
        **{f"{geometry_col}_company_count": ("id", np.count_nonzero)}
    ).reset_index().pivot(
        index="store_name",
        columns="type",
        values=f"{geometry_col}_company_count",
    )
    for c in ["India's 501-1000", "India's Top 500"]:
        if c not in joined.columns.tolist():
            joined[c] = 0
    try:
        joined["India's Top 1000"] = joined[["India's 501-1000", "India's Top 500"]].sum(axis=1)
    except:
        joined["India's Top 1000"] = 0
    joined['company_count'] = joined.sum(axis=1)
    return joined.to_dict('records')


def get_school_college(poly):
    df1 = (
        pd.read_csv("data/bng_school_colleges_final.csv")
        .dropna(subset=["id"])
        .drop_duplicates(subset=["id"])
    )
    mask = df1["category"].isin(["college", "school", ]) & ~df1["verified"].isin(['FALSE'])
    df = df1[mask]
    df_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["lng"], df["lat"]))
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined_ = stores_gdf.sjoin(df_gdf, how="inner", predicate="contains")
    geometry_col = "geometry"
    joined = (
        joined_.groupby(["store_name", "category"])
        .agg(**{f"{geometry_col}_type": ("id", np.count_nonzero)})
        .reset_index()
        .pivot(
            index="store_name",
            columns="category",
            values=f"{geometry_col}_type",
        )
    )
    return joined.to_dict('records')[0]


def get_medical(poly):
    df = (
        pd.read_csv("data/bng_medical_ranked.csv")
        .dropna(subset=["id"])
        .drop_duplicates(subset=["id"])
    )
    df=df.fillna({"date":0,"reviews_per_day":0})
    df_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df["lng"], df["lat"]))
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined_ = stores_gdf.sjoin(df_gdf, how="inner", predicate="contains")
    g = joined_[['id', 'name', 'lat', 'lng', 'source', 'category', 'number_of_votes','reviews_per_day']].groupby(["category"])
    joined_[['id', 'name', 'lat', 'lng', 'source', 'category', 'number_of_votes','reviews_per_day']].to_csv("data/output/hospitals.csv",
                                                                                          index=False)
    dict_list = []
    for name, group in g:
        d = {'category': name[0], 'top_pois': group.to_dict('records')}
        dict_list.append(d)
    top_pois_by_category = pd.DataFrame(dict_list).to_dict(orient='records')
    geometry_col = "geometry"
    joined = (
        joined_.groupby(["store_name", "category"])
        .agg(**{f"{geometry_col}_type": ("id", np.count_nonzero)})
        .reset_index()
        .pivot(
            index="store_name",
            columns="category",
            values=f"{geometry_col}_type",
        )
    )
    co = joined.to_dict('records')[0]
    resp = {}
    resp["count"] = co
    resp['top_pois'] = top_pois_by_category
    return resp


def get_projects(poly):
    projects = (
        pd.read_csv("data/projects.csv")
        .drop_duplicates(subset=["id"])
        .reset_index(drop=True)
    )
    projects['default_units'] = projects['final_num_units'].apply(lambda x: 40 if x == 0.0 else x)
    projects_gdf = gpd.GeoDataFrame(
        projects[["id", "name", "lat", "lng", "final_num_units", "default_units"]],
        geometry=gpd.points_from_xy(projects.lng, projects.lat),
    )
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined_ = stores_gdf.sjoin(projects_gdf, how="inner", predicate="contains")
    geometry_col = "geometry"
    aggs = {
        f"num_units": ("final_num_units", sum),
        f"default_units": ("default_units", sum),
        f"count_projects": ("id", np.count_nonzero),
    }
    joined = joined_.groupby("store_name").agg(**aggs).reset_index()
    joined = joined.drop(columns=["store_name"])
    resp = joined.to_dict(orient="records")[0]
    resp['projects'] = joined_[['id', "name", 'lat', 'lng', 'final_num_units', 'default_units']].sort_values(
        by='final_num_units', ascending=False).to_dict(orient="records")
    return resp


def get_affluence_index(poly):
    aff_ind = pd.read_csv("data/bangalore_1km_grid_affluence_index.csv")
    aff_ind_gdf = gpd.GeoDataFrame(aff_ind[["geoid", "affluence_index"]],
                                   geometry=gpd.GeoSeries.from_wkt(aff_ind['geometry']))
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined = gpd.sjoin(stores_gdf, aff_ind_gdf, how="left", predicate='intersects')
    aff_ind_city = joined.groupby("store_name").agg({"affluence_index": "mean"}).reset_index()
    aff_index = np.mean(joined['affluence_index'])
    return dict(affluence_index=aff_index)


def get_income_distribution(poly):
    aff_ind = pd.read_csv("data/household_income_by_grid_3_with_nni_based_income.csv")
    aff_ind_gdf = gpd.GeoDataFrame(aff_ind, geometry=gpd.GeoSeries.from_wkt(aff_ind['geom']))
    aff_ind_gdf['geom'] = aff_ind_gdf['geometry']  # .apply(shapely.from_wkt)
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined = gpd.sjoin(stores_gdf, aff_ind_gdf, how="left", predicate='intersects')
    cols = ['EWS_HH', 'LIG_HH', 'MIG_HH', 'AIG_HH', 'EIG_HH', ]
    for col in cols:
        joined[f'{col}'] = joined[[col, 'geometry', 'geom']].apply(
            lambda x: int(get_population_of_intersection(x[0], x[1], x[2])), axis=1)
    joined['total_hh'] = joined[cols].sum(axis=1)
    cols = ['EWS_HH', 'LIG_HH', 'MIG_HH', 'AIG_HH', 'EIG_HH', 'total_hh']
    fns = {col: (col, np.sum) for col in cols}
    fns['median_household_income'] = ("avg_household_income", np.median)
    hh_distribution = joined.groupby("store_name").agg(**fns).to_dict('records')
    return hh_distribution


def get_competition(poly):
    malls = pd.read_csv("data/bng_malls_ranked.csv")
    malls = gpd.GeoDataFrame(malls, geometry=gpd.GeoSeries.from_wkt(malls['wkt']))
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined = gpd.sjoin(stores_gdf, malls, how="left", predicate='intersects')
    joined = joined.sort_values(by=['reviews_per_day'], ascending=False)
    joined = joined[['id', 'name', 'number_of_votes', 'ratings', 'lat', 'lng']]
    centroid = poly.centroid
    lat = centroid.y
    lng = centroid.x
    joined['distance'] = joined[['lat', 'lng']].apply(lambda x: round(geodesic((x[0], x[1]), (lat, lng)).km, 2), axis=1)
    joined['type'] = 'Competitor'
    return joined.to_dict('records')


def get_top_brands(poly):
    brands = pd.read_csv("data/bng_branded_data_ranked.csv")
    brands = gpd.GeoDataFrame(brands, geometry=gpd.points_from_xy(brands['lng'], brands['lat']))
    stores_gdf = gpd.GeoDataFrame([{"store_name": 1, "geometry": poly}], geometry='geometry')
    joined = gpd.sjoin(stores_gdf, brands, how="left", predicate='contains')
    joined = joined.groupby(['brand_name', 'category']).agg({'reviews_per_day': np.sum}).reset_index()
    joined = joined.sort_values(by='reviews_per_day', ascending=False)
    joined = joined.to_dict(orient='records')
    return joined


def get_category_count():
    df1 = pd.read_csv('data/non_branded_data_ranked.csv').dropna(subset=['id']).drop_duplicates(subset=['id'])
    df1['brand_id'] = 'N_A'
    df1['brand_name'] = 'N_A'
    df2 = pd.read_csv('data/bng_branded_data_ranked.csv').dropna(subset=['id']).drop_duplicates(subset=['id'])
    df = pd.concat([df1, df2])
    df['brand_name'] = df['brand_name'].str.title()

    df_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['lng'], df['lat']))
    stores = pd.read_csv("data/catchment.csv").dropna(subset=['lat']).drop(columns=['lat', 'lng'])
    stores['store_name'] = 1
    geometry_cols = ['500_buffer', '500_catchment']
    joined_dfs = {}
    for geometry_col in geometry_cols:
        stores_gdf = gpd.GeoDataFrame(
            stores, geometry=gpd.GeoSeries.from_wkt(stores[geometry_col])
        )
        joined_ = stores_gdf.sjoin(df_gdf, how="inner", predicate="contains")
        g = joined_[['id', 'name', 'lat', 'lng', 'source', 'category', 'number_of_votes', 'reviews_per_day']].groupby(
            ['category'])
        dict_list = []
        for name, group in g:
            d = {'category': name[0], 'top_pois': group.to_dict('records')}
            dict_list.append(d)
        top_pois = pd.DataFrame(dict_list)
        joined_['geometry_type'] = geometry_col
        joined1 = joined_.groupby(["store_name", "category"]).agg(
            **{f"count": ("id", np.count_nonzero)}
        ).reset_index()
        joined2 = joined_[joined_["brand_id"] != 'N_A'].groupby(["store_name", "category", 'brand_name']).agg(
            **{f"ranking": ("reviews_per_day", np.sum),
               }
        ).reset_index().groupby(["category", ])[['brand_name', 'ranking']].apply(
            lambda g: dict(map(tuple, g.values.tolist()))).reset_index().rename(columns={0: 'top_brands'})
        joined = joined1.merge(joined2, how="left", on='category').merge(top_pois, how="left", on='category')
        joined = joined.reset_index(drop=True)
        joined = joined[['category', 'count', 'top_brands', 'top_pois']].to_dict(orient='records')
        joined_dfs[geometry_col] = joined
    return joined_dfs


def get_trans_type(desc, fallback):
    if fallback.lower() in ('buy', 'sale', 'resale', 'new property') or 'sale' in desc.lower():
        return 'buy'
    elif fallback.lower() == 'rent' or 'rent' in desc.lower():
        return 'rent'
    else:
        return fallback


def get_prop_type(desc, fallback):
    if 'apartment' in desc.lower() or 'apartment' in fallback.lower():
        return 'AP'
    elif 'flat' in desc.lower():
        return 'AP'
    else:
        return fallback


def get_rent_and_sale_prices():
    df = pd.read_csv("data/raw/bng_properties.csv")
    df = df.fillna({"price": 0, 'type': '', 'trans_type': '', 'desc': '', 'name': ''})
    df['trans_type'] = df[['desc', 'trans_type']].apply(lambda x: get_trans_type(x[0], x[1]), axis=1)
    df['type'] = df[['name', 'type']].apply(lambda x: get_prop_type(x[0], x[1]), axis=1)
    mask = (df['type'] == 'AP') & (df['trans_type'].isin(['buy', 'rent']))
    df = df[mask]
    df_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df['lng'], df['lat']))
    stores = pd.read_csv("data/catchment.csv").dropna(subset=['lat'])
    stores['store_name'] = 1
    geometry_cols = ['500_buffer', '500_catchment']
    # geometry_cols = [ '500_catchment']
    joined_dfs = {}
    for geometry_col in geometry_cols:
        stores_gdf = gpd.GeoDataFrame(
            stores, geometry=gpd.GeoSeries.from_wkt(stores[geometry_col])
        )
        joined_ = stores_gdf.sjoin(df_gdf, how="inner", predicate="contains")
        joined_['geometry_type'] = geometry_col
        joined = joined_.groupby(["store_name", "trans_type"]).agg(
            **{f"median_price": ("price", np.median),
               # f"mean_price": ("price", np.mean)
               }
        ).reset_index().drop(columns=['store_name']).to_dict(orient='records')[0]
        joined_dfs[geometry_col] = joined
    return joined_dfs
