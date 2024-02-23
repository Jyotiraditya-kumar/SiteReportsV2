from concurrent.futures import ProcessPoolExecutor,ThreadPoolExecutor
from data_extraction import QueryAthena

category_wise_count_needed = {
    "vibrancy": {
        "restaurant": 'branded',
        "bar_pub": 'branded',
        'food_other': 'verified',
        'coffee_shop': 'verified',
    },
    'fashion_index': {
        'clothing_store': 'verified',
        'shoe_store': 'verified',
        'jewelry_store': 'verified',
        'cosmetic': 'verified',
        'salon': 'verified',
    },
    'healthcare_index': {
        'hospital': 'branded',
        'pharmacy': 'branded',
        'clinic': 'branded',
    },
    'malls_index': {
        'shopping_mall': 'verified',
    },
    'education_index': {
        'school': 'verified',
        'college': 'verified',
        'tuition_center': 'verified',
    },
    'public_transport_index': {
        'bus_stop': 'total',
        'metro_station': 'total',
    },
    'entertainment_index': {
        'cinema_hall': 'verified',
    },
    'leisure_index': {
        'gym_fitness': 'verified',
        'tourist_attraction': 'verified',
    },
    'grocery_index': {
        "grocery_store": 'verified',
    },
    'supermarket_index': {
        "supermarket": 'verified',
    },
    'religious_index': {
        "religious_place": 'verified',
    },
    'company_index': {
        'Private Sector': 'total',
        'Public Sector': 'total',
        'Govt Sector': 'total',
        'office': 'verified',
    },
    'electronics_index': {
        'electronic_store': 'verified',
    },
    'home_decor_index': {
        'home_decor': 'verified',
    },
    'parks_index': {
        'park': 'total',
    },
    'connectivity_index': {
        'road_area': 'total'
    },
    'apartments_index': {
        'apartments': 'total'
    },
    'affluence_index': {
        'affluence': 'total'
    },
    'competitor': {
        'competitor': 'total'
    },
    'anchor': {
        'anchor': 'total'
    }
}

category_wise_count_needed_weighted = {
    "vibrancy": {
        "restaurant": 0.5,
        "bar_pub": 0.2,
        'food_other': 0,
        'coffee_shop': 0.3

    },
    'fashion_index': {
        'clothing_store': 0.4,
        'shoe_store': 0.3,
        'jewelry_store': 0.2,
        'cosmetic': 0.1,
        'salon': 0.0
    },
    "healthcare_index": {
        'hospital': 0.5,
        'pharmacy': 0.2,
        'clinic': 0.3
    },
    'malls_index': {
        'shopping_mall': 1.0
    },
    'education_index': {
        'school': 0.5,
        'college': 0.4,
        'tuition_center': 0.1
    },
    'connectivity_index': {
        "bus_stop": 0.3,
        "metro_station": 0.2,
        'road_area': 0.5
    },
    'entertainment_index': {
        'cinema_hall': 1.0
    },
    'leisure_index': {
        'gym_fitness': 0.7,
        'tourist_attraction': 0.3
    },
    'grocery_index': {
        "grocery_store": 1.0
    },
    'supermarket_index': {
        "supermarket": 1.0
    },
    'religious_index': {
        "religious_place": 1.0
    },
    'company_index': {
        'Private Sector': 0.7,
        'Public Sector': 0.1,
        'Govt Sector': 0.1,
        'office': 0.1
    },
    'electronics_index': {
        'electronic_store': 1.0
    },
    'home_decor_index': {
        'home_decor': 1.0
    },
    'parks_index': {
        'park': 1.0
    },
    'apartments_index': {
        'apartments': 1.0
    },
    'affluence_index': {
        'affluence': 1.0
    },
    'competitor_index': {
        'competitor': 0.6,
        'anchor': 0.4
    }
}

import awswrangler as wr
import pandas as pd
import h3
from shapely import wkt, geometry as geom
import numpy as np
import random


def get_city_polygon(id: int):
    city_polygon = wr.athena.read_sql_query(
        sql=f"""select geometry from hyperlocal_analysis_ind_dev.ind_top_8_cities_geometry where id={id} """,
        database='hyperlocal_analysis_ind_dev',
        ctas_approach=False,
    )
    return wkt.loads(city_polygon.iloc[0]['geometry'])


def create_hexagon_dataframes_for_city(hexagons, city_id):
    df = pd.DataFrame({'hexagon_id': list(hexagons)})
    df['lat'] = df['hexagon_id'].apply(lambda x: h3.h3_to_geo(x)[0])
    df['lng'] = df['hexagon_id'].apply(lambda x: h3.h3_to_geo(x)[1])
    df['city_id'] = city_id
    return df


def generate_sub_hexagons_for_city(city_id, hexagon_level=9):
    city_polygon = get_city_polygon(city_id)
    subhexagons_for_city = h3.polyfill(city_polygon.__geo_interface__, hexagon_level, geo_json_conformant=True)
    return create_hexagon_dataframes_for_city(subhexagons_for_city, city_id)


def get_isochrone(lat, lng, travel_mode, cost_type, cost,
                  key="pk.eyJ1IjoiYW1hcmRlZXA3NjI2IiwiYSI6ImNsMDlxamJyYTBmdjEzZHF2N2pvY2VjcXYifQ.nzUPgDHBn-eNwDk2T3_jBw"):
    import requests, logging, traceback
    MAPBOX_ACCESS_TOKEN = key
    try:
        if cost_type == "time":
            cost_type = "contours_minutes"
        elif cost_type == "distance":
            cost_type = "contours_meters"
        else:
            raise Exception("cost_type must be either time or distance")

        if travel_mode not in ["driving", "walking", "cycling"]:
            raise Exception("travel_mode must be either driving or walking or cycling")

        mapbox_url = f'https://api.mapbox.com/isochrone/v1/mapbox/{travel_mode}/{lng}%2C{lat}?{cost_type}={cost}&polygons=true&denoise=1&access_token={MAPBOX_ACCESS_TOKEN}'
        logging.debug(mapbox_url)
        response = requests.get(mapbox_url)
        if response.status_code == 200:
            data = response.json()
            data = data["features"][0]["geometry"]["coordinates"][0]
        else:
            # raise Exception("Mapbox API call failed with message: " + response.text)
            return None

        isochrone_polygon = []
        for point in data:
            isochrone_polygon.append(geom.Point(point[0], point[1]))
        isochrone_polygon = geom.Polygon(isochrone_polygon)

        return isochrone_polygon
    except Exception as e:
        # logging.error(e)
        # logging.error(traceback.format_exc() )

        # raise Exception("Failed to fetch Isochrone Polygon")
        return None


def get_city_isochrone_poi_counts(city_id):
    query_to_get_poi_count_by_category_and_index = f""" 
        select city_id,
        isochrone_id,
        isochrone_type,
        category,
        sum(
            (
                CASE
                    WHEN (
                        (brand_id IS NOT NULL)
                        AND (brand_id <> 'N_A')
                    ) THEN 1 ELSE 0
                END
            )
        ) branded,
        sum(
            (
                CASE
                    WHEN (verified IN (1, 2, 4)) THEN 1 ELSE 0
                END
            )
        ) verified,
        count(id) total,
        sum(
            (
                CASE
                    WHEN (number_of_votes > 50) THEN 1 ELSE 0
                END
            )
        ) high_voted_gt_50
    from (
            select *
            from (
                    select *
                    from hyperlocal_analysis_ind_dev.ind_poi_data_v2
                    where active = 1
                ) A
                cross join (
                    select city_id,
                        isochrone_id,
                        isochrone_type,
                        geometry
                    from hyperlocal_analysis_ind_dev.ind_city_isochrones
                    where city_id = {city_id}
                ) B
            where st_intersects(
                    st_point(A.lng, A.lat),
                    st_geometryfromtext(B.geometry)
                )
        )
    group by city_id,
        isochrone_id,
        isochrone_type,
        category
    """
    return wr.athena.read_sql_query(
        sql=query_to_get_poi_count_by_category_and_index,
        database='hyperlocal_analysis_ind_dev',
        ctas_approach=True,
    )


def get_city_parks_isochrone_poi_counts(city_id):
    parks_city_isochrone_poi_counts_query = f"""
        select city_id,
        isochrone_id,
        isochrone_type,
        category,
        0 as branded,
        0 as verified,
        sum(area) / 2e5 as total,
        0 as high_voted_gt_50
        from (
            select A.place_id as id,
                A.lat,
                A.lng,
                A.park_polygon,
                case
                    when A.area > 0 then A.area else 7755
                end as area,
                'park' as category,
                B.city_id,
                B.isochrone_id,
                B.isochrone_type
            from datasets_prep.parks A 
            cross join (
                select city_id,
                    isochrone_id,
                    isochrone_type,
                    geometry
                from hyperlocal_analysis_ind_dev.ind_city_isochrones
                where city_id = {city_id}
            ) B
            where st_intersects(
                st_point(A.lng, A.lat),
                st_geometryfromtext(B.geometry)
            )
            and active = 1
        )
        group by city_id,
        isochrone_id,
        isochrone_type,
        category
    """
    return wr.athena.read_sql_query(
        sql=parks_city_isochrone_poi_counts_query,
        database='hyperlocal_analysis_ind_dev',
        ctas_approach=True,
    )


def get_city_aparments_isochrone_poi_counts(city_id):
    city_apartment_isochrone_poi_counts_query = f"""
        select city_id,
        isochrone_id,
        isochrone_type,
        category,
        0 as branded,
        0 as verified,
        sum(default_units)/2e3 as total,
        0 as high_voted_gt_50
        from (
            select A.id,
                A.lat,
                A.lng,
                A.default_units,
                'apartments' as category,
                B.city_id,
                B.isochrone_id,
                B.isochrone_type
            from datasets_prep.ind_residential_projects_gold A 
            cross join (
                select city_id,
                    isochrone_id,
                    isochrone_type,
                    geometry
                from hyperlocal_analysis_ind_dev.ind_city_isochrones
                where city_id = {city_id}
            ) B
            where st_intersects(
                st_point(A.lng, A.lat),
                st_geometryfromtext(B.geometry)
            )
        )
        group by city_id,
            isochrone_id,
            isochrone_type,
            category
    """
    return wr.athena.read_sql_query(
        sql=city_apartment_isochrone_poi_counts_query,
        database='hyperlocal_analysis_ind_dev',
        ctas_approach=True,
    )


def get_categories_and_count_type_needed():
    all_category_wise_count_needed_pair = {}
    for k, v in category_wise_count_needed.items():
        for k1, v1 in v.items():
            all_category_wise_count_needed_pair[k1] = v1

    return all_category_wise_count_needed_pair


def get_index_category_weights():
    all_category_wise_count_needed_pair = []
    for key, value in category_wise_count_needed_weighted.items():
        for k, v in value.items():
            all_category_wise_count_needed_pair.append((key, k, v))
    return all_category_wise_count_needed_pair


def percentile(n):
    def percentile_(x):
        return np.percentile(x, n)

    percentile_.__name__ = 'percentile_%s' % n
    return percentile_


def fetch_city_counts_for_competitors(city_id, competitor_brand_ids=[], anchor_brand_ids=[]):
    competitor_brand_ids = competitor_brand_ids + [str(random.randint(10000, 100000)),
                                                   str(random.randint(10000, 100000))] if len(
        competitor_brand_ids) < 2 else competitor_brand_ids
    anchor_brand_ids = anchor_brand_ids + [str(random.randint(10000, 100000)),
                                           str(random.randint(10000, 100000))] if len(
        anchor_brand_ids) < 2 else anchor_brand_ids

    city_query = f"""
    select 
        city_id,
        isochrone_id,
        isochrone_type,
        category,
        0 as branded ,
        0 as verified,
        count(*) as total,
        0 as high_voted_gt_50
    from 
    (
        select 
            case when A.brand_id in {tuple(competitor_brand_ids)} then 'competitor'
            when A.brand_id in {tuple(anchor_brand_ids)} then 'anchor' 
            else 'other' end as category,
            B.city_id ,
            B.isochrone_id,
            B.isochrone_type
        from  hyperlocal_analysis_ind_dev.ind_poi_data_v2 A 
        cross join (
           select city_id,
                isochrone_id,
                isochrone_type,
                geometry
            from hyperlocal_analysis_ind_dev.ind_city_isochrones
            where city_id = {city_id}
        ) B
        where st_intersects(
                st_point(A.lng, A.lat),
                st_geometryfromtext(B.geometry)
            )
        and A.brand_id in {tuple(competitor_brand_ids + anchor_brand_ids)}
        and A.active = 1
        )
        group by city_id,
    isochrone_id,
    isochrone_type,
    category
    """
    data = wr.athena.read_sql_query(city_query, database='hyperlocal_analysis_ind_dev', ctas_approach=False)
    if len(data) == 0:
        return pd.DataFrame(
            columns=['city_id', 'isochrone_id', 'isochrone_type', 'category', 'branded', 'verified', 'total',
                     'high_voted_gt_50'])

    data = data[data['category'].isin(get_categories_and_count_type_needed().keys())]
    data['count_needed'] = data['category'].apply(lambda x: get_categories_and_count_type_needed()[x])
    data['count'] = data.apply(lambda x: x[x['count_needed']] if x['count_needed'] is not None else None, axis=1)
    category_vals = data.groupby(['city_id', 'isochrone_type', 'category']).agg({'count': ['max', 'min', 'median',
                                                                                           'mean', 'std',
                                                                                           percentile(99),
                                                                                           percentile(98), percentile(
            95)]}).reset_index()

    category_vals['type'] = 'category'

    category_vals.columns = ['city_id', 'isochrone_type', 'name', 'max', 'min', 'median', 'mean', 'std',
                             'percentile_99', 'percentile_98', 'percentile_95', 'type']

    return category_vals


def fetch_isochrone_counts_for_competitors(isochrone_polygon, competitor_brand_ids=[], anchor_brand_ids=[], ):
    competitor_brand_ids = competitor_brand_ids + [str(random.randint(10000, 100000)),
                                                   str(random.randint(10000, 100000))] if len(
        competitor_brand_ids) < 2 else competitor_brand_ids
    anchor_brand_ids = anchor_brand_ids + [str(random.randint(10000, 100000)),
                                           str(random.randint(10000, 100000))] if len(
        anchor_brand_ids) < 2 else anchor_brand_ids

    isochrone_query = f"""
    select 
        category,
        0 as branded ,
        0 as verified,
        count(*) as total,
        0 as high_voted_gt_50
    from
    (
    select
        case when A.brand_id in {tuple(competitor_brand_ids)} then 'competitor'
            when A.brand_id in {tuple(anchor_brand_ids)} then 'anchor' 
            else 'other' end as category
        from hyperlocal_analysis_ind_dev.ind_poi_data_v2 A 
        where st_intersects(
                st_point(A.lng, A.lat),
                st_geometryfromtext('""" + isochrone_polygon.wkt + f"""')
            )
            and A.brand_id in {tuple(competitor_brand_ids + anchor_brand_ids)}
            and A.active = 1
        ) group by category
    """
    # data = wr.athena.read_sql_query(isochrone_query, database='hyperlocal_analysis_ind_dev', ctas_approach=False)
    return isochrone_query


def geom_area_in_sqkm(geom):
    import contextlib
    import io
    import sys
    null_io = io.StringIO()
    with contextlib.redirect_stdout(null_io):
        if type(geom) == str:
            geom = wkt.loads(geom)
        from pyproj import Geod
        geod = Geod(ellps="WGS84")
        area = geod.geometry_area_perimeter(geom)[0] / 10 ** 6
    return -area if area < 0 else area


def get_isochrone_and_area(lat, lng, cost, type='driving', cost_type='time'):
    isochrone_polygon = get_isochrone(lat, lng, type, cost_type, cost)
    return isochrone_polygon, geom_area_in_sqkm(isochrone_polygon)


def read_athena_query(query, database='hyperlocal_analysis_ind_dev', ctas_approach=False):
    # return wr.athena.read_sql_query(query, database=database, ctas_approach=ctas_approach)
    query=QueryAthena(query,database=database)
    return query.run_query()


def fetch_pois_count_for_isochrone(isochrone_polygon, competitor_ids=[], anchor_ids=[]):
    query_for_pois_in_isochrone = """ select category
    , sum((CASE WHEN ((brand_id IS NOT NULL) AND (brand_id <> 'N_A')) THEN 1 ELSE 0 END)) branded
    , sum((CASE WHEN (verified IN (1, 2, 4)) THEN 1 ELSE 0 END)) verified
    , count(id) total
    , sum((CASE WHEN (number_of_votes > 50) THEN 1 ELSE 0 END)) high_voted_gt_50
    from (
            select *
            from hyperlocal_analysis_ind_dev.ind_poi_data_v2
            where st_intersects(
                    st_point(lng, lat),
                    st_geometryfromtext('""" + isochrone_polygon.wkt + """')
        ) ) group by category"""

    parks_data_isochrone_query = """ select 
    'park' as category,
    0 as branded ,
    0 as verified,
        sum(area)/2e5 as total,
        0 as high_voted_gt_50
    from (
        select * from datasets_prep.parks A
        where st_intersects(
                st_point(A.lng, A.lat),
                st_geometryfromtext('""" + isochrone_polygon.wkt + """')
            )
            and active = 1
        ) """

    road_area_grided_query = """ select
        'road_area' as category,
        0 as branded ,
        0 as verified,
        (sum(road_area) * 50) / sum(total_area) as total,
        0 as high_voted_gt_50
        from 
        "hyperlocal_analysis_ind_dev"."ind_road_covered_area_gridded" A 
        where st_intersects(
            st_geometryfromtext('""" + isochrone_polygon.wkt + """'),
            st_geometryfromtext(A.polygon)
            )
    """

    aparments_data_isochrone_query = """ select 
    'apartments' as category,
    0 as branded ,
    0 as verified,
        sum(default_units)/2e3 as total,
        0 as high_voted_gt_50
    from (
        select * from datasets_prep.ind_residential_projects_gold A
        where st_intersects(
                st_point(A.lng, A.lat),
                st_geometryfromtext('""" + isochrone_polygon.wkt + """')
            )
        ) 

    """

    affluence_query = """ 
    select 
    'affluence' as category,
    0 as branded ,
    0 as verified,
        avg(affluence_index) as total,
        0 as high_voted_gt_50
    from (
        select affluence_index from "hyperlocal_analysis_ind_dev"."ind_geoid_indices" A
        where st_intersects(
               st_geometryfromtext(geometry),
                st_geometryfromtext('""" + isochrone_polygon.wkt + """')
            )
            and type = '1km_grid'
        )  """

    competitor_anchors_query = fetch_isochrone_counts_for_competitors(isochrone_polygon, competitor_ids, anchor_ids)
    queries = {"all_pois_counts": query_for_pois_in_isochrone, "parks_count": parks_data_isochrone_query,
               "road_area_counts": road_area_grided_query, "apartments_count": aparments_data_isochrone_query,
               "affluence_query": affluence_query, "competitors_anchors": competitor_anchors_query}
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(read_athena_query, queries.values())
        results = list(results)
    d = dict(zip(queries.keys(), results))
    d['all_pois_counts'] = d['all_pois_counts'].query("category!='park'")

    # all_pois_counts = all_pois_counts.append(park_counts).append(road_area_counts).append(aparments_counts).append(
    #     affluence_query).append(competitor_anchors)
    # all_pois_counts = pd.concat(
    #     [all_pois_counts, park_counts, road_area_counts, aparments_counts, affluence_query, competitor_anchors])
    all_pois_counts = pd.concat(d.values())

    all_category_wise_count_needed_pair = get_categories_and_count_type_needed()
    all_pois_counts['count_needed'] = all_pois_counts[['category', 'branded', 'verified', 'total']].apply(
        lambda x: all_category_wise_count_needed_pair[x['category']] if x[
                                                                            'category'] in all_category_wise_count_needed_pair.keys() else None,
        axis=1)
    all_pois_counts['count'] = all_pois_counts[['category', 'branded', 'verified', 'total', 'count_needed']].apply(
        lambda x: x[x['count_needed']] if x['count_needed'] is not None else None, axis=1)
    all_pois_counts = all_pois_counts[all_pois_counts['count_needed'].notnull()][['category', 'count', ]]
    # all_pois_counts = all_pois_counts.set_index('category').to_dict()['count']
    return all_pois_counts


def create_indexs_from_counts(city_max_with_competitor, isochrone_index_counts, city_id, isochrone_type,
                              percentile_value):
    city_max_with_competitor_subset = city_max_with_competitor[
        (city_max_with_competitor['isochrone_type'] == isochrone_type) &
        (city_max_with_competitor['type'] == 'category') &
        (city_max_with_competitor['city_id'] == city_id)][['name', percentile_value]].rename(
        columns={percentile_value: 'value'})

    city_max_with_competitor_subset = city_max_with_competitor_subset.set_index('name').to_dict()['value']
    city_max_with_competitor_subset['road_area'] = 0.30 * 50
    city_max_with_competitor_subset['affluence'] = 5

    isochrone_index_counts = isochrone_index_counts.set_index('category').to_dict()['count']

    category_indexs = {}
    for city_count_key, city_value in city_max_with_competitor_subset.items():
        if city_count_key == 'affluence':
            category_indexs[city_count_key] = isochrone_index_counts[city_count_key]
            continue
        # if city_count_key=='road_area':
        #    category_indexs[city_count_key] = round( (100 * (isochrone_index_counts[city_count_key] /city_value ) ) / 20, 2)
        #    continue

        if city_count_key not in isochrone_index_counts.keys():
            isochrone_index_counts[city_count_key] = 0

        isochrone_value = isochrone_index_counts[city_count_key]

        if int(city_value) in [0, 1] and int(isochrone_value) == 0:
            category_indexs[city_count_key] = 0
            continue
        if isochrone_value <= 1 or city_value <= 1:
            isochrone_value += 2
            city_value += 2

        if city_value < isochrone_value:
            category_indexs[city_count_key] = 5
        else:
            category_indexs[city_count_key] = round((100 * (np.log(isochrone_value) / np.log(city_value))) / 20, 2)

    return category_indexs


def create_grouped_indexes_from_indexs(indexs):
    all_category_wise_count_needed_pair = []
    for key, value in category_wise_count_needed_weighted.items():
        for k, v in value.items():
            all_category_wise_count_needed_pair.append((key, k, v, indexs.get(k, -1)))

    all_category_wise_count_needed_pair_df = pd.DataFrame(all_category_wise_count_needed_pair,
                                                          columns=['index_type', 'category', 'weight', 'index'])
    all_category_wise_count_needed_pair_df['weighted_index'] = all_category_wise_count_needed_pair_df['index'] * \
                                                               all_category_wise_count_needed_pair_df['weight']

    all_category_wise_count_needed_pair_df = all_category_wise_count_needed_pair_df[
        all_category_wise_count_needed_pair_df['index'] != -1]
    all_category_wise_count_needed_pair_df = all_category_wise_count_needed_pair_df.groupby('index_type').agg(
        {'weighted_index': 'sum', 'weight': 'sum'}).reset_index()
    all_category_wise_count_needed_pair_df['index'] = all_category_wise_count_needed_pair_df['weighted_index'] / \
                                                      all_category_wise_count_needed_pair_df['weight']
    return all_category_wise_count_needed_pair_df[['index_type', 'index']].round(2).set_index('index_type').to_dict()[
        'index']


def create_revenue_score_from_indexs(grouped_indexs, weights_dict):
    score = 0
    for k, v in grouped_indexs.items():
        if k in weights_dict.keys():
            score += v * weights_dict[k]
    return score / sum(weights_dict.values())


def read_city_max_data(city_id):
    city_max_query = f""" select * from hyperlocal_analysis_ind_dev.ind_city_isochrone_max where city_id = {city_id} """
    return wr.athena.read_sql_query(city_max_query, database='hyperlocal_analysis_ind_dev', ctas_approach=False)


def generate_revenue_score(lat, lng, travel_mode, cost_type, cost, catchment_type, competitor_brand_ids=[],
                           anchor_brand_ids=[],
                           location_score_weights=None, *args, **kwargs):
    if not location_score_weights:
        location_score_weights = {
            'competitor_index': 0.20,
            'affluence_index': 0.19,
            'apartments_index': 0.19,
            'fashion_index': 0.19,
            'vibrancy': 0.14,
            'healthcare_index': 0.10,
            'company_index': 0.10,
            'malls_index': 0.05,
            'supermarket_index': 0.04,
        }
    city_id = 3
    isochrone_cost = cost
    isochrone_type = travel_mode
    isochrone_cost_type = cost_type
    isochrone_type_name = catchment_type
    isochrone_polygon, isochrone_area = get_isochrone_and_area(lat, lng, isochrone_cost, isochrone_type,
                                                               isochrone_cost_type)

    city_max = read_city_max_data(city_id)
    competitor_city_max = fetch_city_counts_for_competitors(3, competitor_brand_ids,
                                                            anchor_brand_ids)
    city_max_with_competitor = pd.concat([city_max, competitor_city_max])

    poi_counts_for_isochrone = fetch_pois_count_for_isochrone(
        isochrone_polygon,
        competitor_brand_ids,
        anchor_brand_ids
    )
    indexs = create_indexs_from_counts(
        city_max_with_competitor,
        poi_counts_for_isochrone,
        city_id,
        isochrone_type_name,
        'percentile_95'
    )

    grouped_indexs = create_grouped_indexes_from_indexs(indexs)
    revenue_score = create_revenue_score_from_indexs(grouped_indexs, location_score_weights) * 20
    return {"revenue_score": revenue_score,"grouped_indexs": grouped_indexs}


if __name__ == "__main__":
    city_id = 3
    cost = 15
    travel_mode = 'driving'
    cost_type = 'time'
    catchment_type = 'i15mind'
    lat = 13.095999774716756
    lng = 77.57916090477161
    competitor_brand_ids = []
    anchor_brand_ids = []
    weights_dict = {
        'competitor_index': 0.20,
        'affluence_index': 0.19,
        'apartments_index': 0.19,
        'fashion_index': 0.19,
        'vibrancy': 0.14,
        'healthcare_index': 0.10,
        'company_index': 0.10,
        'malls_index': 0.05,
        'supermarket_index': 0.04,
    }
    revenue_score, grouped_indexes = generate_revenue_score(lat, lng, travel_mode=travel_mode, cost_type=cost_type,
                                                            cost=cost, catchment_type=catchment_type,
                                                            competitor_brand_ids=competitor_brand_ids,
                                                            anchor_brand_ids=anchor_brand_ids,
                                                            weight_score=weights_dict)
    print(revenue_score, grouped_indexes)
