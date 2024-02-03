import json
import os

import pandas as pd
from pyforest import *

import numpy as np
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import dendrogram, linkage
import math
from ast import Dict, Tuple
from typing import Any
import numpy as np
from scipy.spatial import Voronoi
from shapely.geometry import Polygon
from math import radians, cos, sin, asin, sqrt, atan2, degrees
import alphashape
from pyquadkey2 import quadkey
import itertools
import math
import shapely as geom
import shapely
from shapely import wkt


def split_large_polygon_by_density(polygon, points, base_polygon_area=0):
    if type(polygon) == str:
        polygon = wkt.loads(polygon)
    try:
        from sklearn.cluster import KMeans
        polygon_area = polygon.area
        if polygon_area < base_polygon_area:
            return [polygon]
        else:
            num_splits = min(int(polygon_area / base_polygon_area), 4)
            kmeans = KMeans(n_clusters=num_splits)
            kmeans.fit(points)

            cluster_labels = kmeans.labels_
            cluster_centers = kmeans.cluster_centers_
            points_by_poly = []
            for i in np.unique(cluster_labels):
                points_by_poly.append(points[cluster_labels == i])

            polygons = []
            for points in points_by_poly:
                polygon = geom.MultiPoint([geom.Point(i[0], i[1]) for i in points]).convex_hull.simplify(0.001)
                polygons.append(polygon)

            return polygons
    except:
        return [polygon]


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


def get_street_name_locality(lat, lng,
                             mapbox_key='pk.eyJ1IjoibGFpcjA4MjYiLCJhIjoiY2tkcGoxcnRzMDZvODJxbXk0MWhlcWN2aSJ9.5-yjt_SUq4w5JII7CvD4cA'):
    import requests
    BASE_URL = f"https://api.mapbox.com/search/geocode/v6/reverse?longitude={lng}&latitude={lat}&access_token={mapbox_key}"
    try:
        response = requests.get(BASE_URL)
    except:
        return "", None
    data = response.json()
    if len(data['features']) > 0:
        data_1 = data['features'][0]
        # print(data_1['properties']['context'])
        if 'street' in data_1['properties']['context'].keys():
            street_name = data_1['properties']['context']['street']['name']
        else:
            street_name = ""

        if 'neighborhood' in data_1['properties']['context'].keys():
            neighbourhood = ", " + data_1['properties']['context']['neighborhood']['name']
        else:
            neighbourhood = ""

        if 'locality' in data_1['properties']['context'].keys():
            locality = data_1['properties']['context']['locality']['name']
        else:
            locality = None
        return street_name + neighbourhood, locality
    else:
        return "", None


def get_relative_direction(x1, y1, x2, y2):
    # Calculate the angle between the two points in radians
    angle = math.atan2(y2 - y1, x2 - x1)

    # Convert the angle from radians to degrees
    angle_degrees = math.degrees(angle)

    # Convert the angle to a positive value between 0 and 360 degrees
    angle_degrees = (angle_degrees + 360) % 360
    directions = ["North", "East", "South", "West"]

    # Calculate the index of the direction based on the angle
    index = round(angle_degrees / 45) % 4

    # Return the relative direction
    return directions[index]


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
            # logging.debug(data)
            data = data["features"][0]["geometry"]["coordinates"][0]
        else:
            raise Exception("Mapbox API call failed with message: " + response.text)

        isochrone_polygon = []
        for point in data:
            isochrone_polygon.append(geom.Point(point[0], point[1]))
        isochrone_polygon = geom.Polygon(isochrone_polygon)

        return isochrone_polygon
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())

        raise Exception("Failed to fetch Isochrone Polygon")


def buffer_a_polygon_in_meters(polygon, meters):
    import pyproj
    from shapely.ops import transform

    wgs84 = pyproj.CRS('EPSG:4326')
    utm = pyproj.CRS('EPSG:32618')
    project_1 = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform
    project_2 = pyproj.Transformer.from_crs(utm, wgs84, always_xy=True).transform

    if type(polygon) == str:
        polygon = wkt.loads(polygon)
    if meters > 0:
        polygon = transform(project_1, polygon)
        polygon = polygon.buffer(meters)
        polygon = transform(project_2, polygon)
    return polygon


def buffer_polygons(polygon, min, max, buffer_area_cutt_off):
    if type(polygon) == str:
        polygon = wkt.loads(polygon)
    area = geom_area_in_sqkm(polygon) * 1e6
    if area < buffer_area_cutt_off:
        amount_to_buffer = np.sqrt((buffer_area_cutt_off - area) / (2 * 3.14))
        amount_to_buffer = np.clip(amount_to_buffer, min, max)
        polygon = buffer_a_polygon_in_meters(polygon, amount_to_buffer)
    return polygon


def get_num_optimal_clusters_for_a_city(bounds_polygon, base_zone_area_km_2=300):
    area_bounds = geom_area_in_sqkm(bounds_polygon)
    num_zones = area_bounds / base_zone_area_km_2
    return int(num_zones)


def get_optimal_r_in_meters_for_central_polygon(bounds_polygon, base_zone_area_km_2=300):
    r = np.sqrt((base_zone_area_km_2 * 1e6) / (2 * 3.14))
    return r


def haversine(lat1, lon1, lat2, lon2):
    # Haversine formula to calculate the distance between two points
    R = 6371000  # Earth radius in meters
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) * sin(dlat / 2) + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) * sin(dlon / 2)
    c = 2 * asin(sqrt(a))
    distance = R * c
    return distance


def calculate_new_coordinates(lat, lon, distance, bearing):
    # Calculate new coordinates based on distance and bearing
    R = 6371000  # Earth radius in meters
    lat_rad = radians(lat)
    lon_rad = radians(lon)
    angular_distance = distance / R
    bearing_rad = radians(bearing)

    new_lat_rad = asin(sin(lat_rad) * cos(angular_distance) + cos(lat_rad) * sin(angular_distance) * cos(bearing_rad))
    new_lon_rad = lon_rad + atan2(sin(bearing_rad) * sin(angular_distance) * cos(lat_rad),
                                  cos(angular_distance) - sin(lat_rad) * sin(new_lat_rad))

    new_lat = degrees(new_lat_rad)
    new_lon = degrees(new_lon_rad)

    return new_lat, new_lon


def get_points_around_center(center_lat, center_lon, distance):
    # Calculate points in north, south, east, and west directions
    north_west = calculate_new_coordinates(center_lat, center_lon, distance, 315)
    south_east = calculate_new_coordinates(center_lat, center_lon, distance, 135)
    north_east = calculate_new_coordinates(center_lat, center_lon, distance, 45)
    south_west = calculate_new_coordinates(center_lat, center_lon, distance, 225)

    return north_west, south_east, north_east, south_west


def get_polygon_empty_areas_of_zones(all_zones_polygon, total_city_bouding_box_polygon):
    empty_areas = total_city_bouding_box_polygon
    for zone in all_zones_polygon:
        empty_areas = empty_areas.difference(zone)
    return empty_areas.geoms


def voronoi_polygons(polygon, num_points):
    # Generate random points within the bounding box of the polygon
    min_x, min_y, max_x, max_y = polygon.bounds
    points = np.random.uniform(low=min_x, high=max_x, size=(num_points, 2))

    # Add the vertices of the polygon to the points
    polygon_vertices = np.array(polygon.exterior.coords)
    points = np.vstack([points, polygon_vertices])

    # Create Voronoi diagram
    vor = Voronoi(points)

    # Create Voronoi polygons
    polygons = []
    for region in vor.regions:
        if -1 not in region and len(region) > 0:
            polygon_coords = [vor.vertices[i] for i in region]
            polygons.append(Polygon(polygon_coords).intersection(polygon))

    return polygons


def generate_all_valid_vernoi_polygons_for_empty_geoms(empty_geoms, num_points=50):
    all_vernoi_polygons = []
    for empty_geom in empty_geoms:
        curr_vernoi_polygons = voronoi_polygons(empty_geom, num_points)
        all_vernoi_polygons.extend([polygon for polygon in curr_vernoi_polygons if not polygon.is_empty])

    return all_vernoi_polygons


def add_a_polygon_to_a_zone(zones_geoms: Dict, valid_vernoi_polygons):
    # check distance of each polygon center from each zone edge and add the polygon to the zone with minimum distance
    for polygon in valid_vernoi_polygons:
        min_d = 1e9
        min_zone = None
        zone_id_ = None
        for zone_id, zone in zones_geoms.items():
            if zone_id == 4:
                continue
            d = polygon.centroid.distance(zone)
            if d < min_d:
                min_d = d
                min_zone = zone
                zone_id_ = zone_id
        zones_geoms[zone_id_] = min_zone.union(polygon)
    return zones_geoms


def remove_clip_from_central_zone(id, geometry, central_geometry, central_zone_id):
    if id != central_zone_id:
        return geometry.difference(central_geometry)
    else:
        return geometry


def prepare_zone_dict(zones_df):
    data_d = zones_df[['cluster', 'geometry']].to_dict(orient='records')
    zones_dict = {}
    for d in data_d:
        zones_dict[d['cluster']] = d['geometry']

    return zones_dict


def get_points_from_polygon_multipolygon_geometry_collection(geometry_collection):
    import shapely.geometry as geom
    points = []
    if type(geometry_collection) != geom.GeometryCollection and type(geometry_collection) != geom.MultiPolygon:
        for k in geometry_collection.exterior.coords:
            points.append((k[0], k[1]))
        return points

    for geo in geometry_collection.geoms:
        if type(geo) == geom.Polygon:
            for point in geo.exterior.coords:
                points.append((point[0], point[1]))
        elif type(geo) == geom.MultiPolygon:
            for polygon in geo.geoms:
                for point in polygon.exterior.coords:
                    points.append((point[0], point[1]))
    return points


def concave_hull(polygon):
    points = get_points_from_polygon_multipolygon_geometry_collection(polygon)

    alpha_shape = alphashape.alphashape(points, 0.001)
    return alpha_shape


def get_concat_hull_for_zones(zones_dict):
    for k, v in zones_dict.items():
        zones_dict[k] = concave_hull(v)
    return zones_dict


def subtract_each_polygon_from_another(return_zones):
    for i in range(len(return_zones)):
        if i == 4:
            continue

        for j in range(len(return_zones)):
            if i != j:
                return_zones[i] = return_zones[i].difference(return_zones[j])
    return return_zones


def quadkey_to_wkt(qk):
    qk = quadkey.QuadKey(qk)
    ne = qk.to_geo(quadkey.TileAnchor.ANCHOR_NE)
    sw = qk.to_geo(quadkey.TileAnchor.ANCHOR_SW)
    nw = qk.to_geo(quadkey.TileAnchor.ANCHOR_NW)
    se = qk.to_geo(quadkey.TileAnchor.ANCHOR_SE)

    return geom.box(nw[1], nw[0], se[1], se[0])


def n_lenght_all_combi(n):
    available_chars = ['0', '1', '2', '3']
    return ["".join(i) for i in list(itertools.product(available_chars, repeat=n))]


def generate_n_lvl_neighbour_quadkeys(quadkey, n):
    # n = 0 means the same quadkey
    # n= 1 means the 8 neighbours of the quadkey and qaudkey itself
    # n = 2 means the 8 neighbours of the quadkey and qaudkey itself and the 8 neighbours of the neighbours

    current_lvl = len(quadkey)
    if current_lvl < n + 1:
        return []

    if n == 0:
        return [quadkey]

    lvls_to_skip = quadkey[:-n]
    all_neighbours_lvl_n = n_lenght_all_combi(n)
    return [lvls_to_skip + i for i in all_neighbours_lvl_n]


def get_level_for_n_grids(n_grids):
    return math.ceil(math.log(n_grids, 4))


def total_fn(series_custome_series):
    return len([i for i in series_custome_series if i[0] == 1])


def branded_fn(series_custome_series):
    return len([i for i in series_custome_series if i[1] is not None and i[1] != 'N_A'])


def verified_fn(series_custome_series):
    return len([i for i in series_custome_series if i[2] in [1, 2, 4]])


def gt_50votes_fn(series_custome_series):
    return len([i for i in series_custome_series if i[3] >= 50])


def get_categories_and_count_type_needed():
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
        }
    }

    all_category_wise_count_needed_pair = {}
    for k, v in category_wise_count_needed.items():
        for k1, v1 in v.items():
            all_category_wise_count_needed_pair[k1] = v1

    return all_category_wise_count_needed_pair


def get_city_grid_level_counts(city_id, level_to_get_neighbours, quantile=0.95):
    base_count_data = wr.athena.read_sql_query(
        """ SELECT * FROM "hyperlocal_analysis_ind_dev"."ind_poi_counts_by_city_quadkeys" where city_id={} """.format(
            city_id),
        database='hyperlocal_analysis_ind_dev',
    )
    parks_data_city_query = """ select 
        lvl_16_quadkey
        , category
        , city_id
        , city_name,
        0 as branded ,
        0 as verified,
        sum(area)/2e5 as total,
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
                    B.id as city_id,
                    B.name as city_name,
                    bing_tile_quadkey(bing_tile_at(A.lat, A.lng, 16)) lvl_16_quadkey
                from datasets_prep.parks A
                    cross join (
                        select *
                        from hyperlocal_analysis_ind_dev.ind_top_8_cities_geometry
                        where id = 3
                    ) B
                where st_intersects(
                        st_point(A.lng, A.lat),
                        st_geometryfromtext(B.geometry)
                    )
                    and active = 1
            )
        group by lvl_16_quadkey,
            category,
            city_id,
            city_name """

    apparments_query = """  select 
        lvl_16_quadkey
        , category
        , city_id
        , city_name,
        0 as branded ,
        0 as verified,
        sum(default_units) / 2e3 as total,
        0 as high_voted_gt_50
        from (
                select A.id as id,
                    A.lat,
                    A.lng,
                    A.default_units,
                    'apartments' as category,
                    B.id as city_id,
                    B.name as city_name,
                    bing_tile_quadkey(bing_tile_at(A.lat, A.lng, 16)) lvl_16_quadkey
                from datasets_prep.bng_residential_projects A
                    cross join (
                        select *
                        from hyperlocal_analysis_ind_dev.ind_top_8_cities_geometry
                        where id = 3
                    ) B
                where st_intersects(
                        st_point(A.lng, A.lat),
                        st_geometryfromtext(B.geometry)
                    )
            )
        group by lvl_16_quadkey,
            category,
            city_id,
            city_name """

    base_count_data = base_count_data[base_count_data['category'] != 'park']
    parks_count_data = wr.athena.read_sql_query(parks_data_city_query, database='hyperlocal_analysis_ind_dev', )
    apparments_count_data = wr.athena.read_sql_query(apparments_query, database='hyperlocal_analysis_ind_dev', )
    # base_count_data = base_count_data.append(parks_count_data).append(apparments_count_data)
    base_count_data = pd.concat([base_count_data, parks_count_data, apparments_count_data])

    all_category_wise_count_needed_pair = get_categories_and_count_type_needed()
    base_count_data['count_needed'] = base_count_data[['category', 'branded', 'verified', 'total']].apply(
        lambda x: all_category_wise_count_needed_pair[x['category']] if x[
                                                                            'category'] in all_category_wise_count_needed_pair.keys() else None,
        axis=1)
    base_count_data['count'] = base_count_data[['category', 'branded', 'verified', 'total', 'count_needed']].apply(
        lambda x: x[x['count_needed']] if x['count_needed'] is not None else None, axis=1)
    base_count_data_filtered = base_count_data[base_count_data['count_needed'].notnull()][
        ['lvl_16_quadkey', 'category', 'count', ]]
    base_count_data_filtered = base_count_data_filtered.pivot(index='lvl_16_quadkey', columns='category',
                                                              values='count').reset_index().fillna(0)

    neighbour_df = base_count_data_filtered[['lvl_16_quadkey']]
    neighbour_df['level_neighbours'] = neighbour_df['lvl_16_quadkey'].apply(
        lambda x: generate_n_lvl_neighbour_quadkeys(x, level_to_get_neighbours))
    neighbour_df = neighbour_df.explode('level_neighbours')
    joined = neighbour_df.merge(base_count_data_filtered, left_on='level_neighbours', right_on='lvl_16_quadkey',
                                how='left')
    joined = joined[joined['lvl_16_quadkey_y'].notnull()]

    agg_dict = {i: 'sum' for i in joined.columns if
                i not in ['lvl_16_quadkey_x', 'level_neighbours', 'lvl_16_quadkey_y']}
    agg_dict['lvl_16_quadkey_y'] = 'count'

    city_quantiles = joined.groupby('lvl_16_quadkey_x').agg(agg_dict).quantile(quantile).reset_index().to_dict(
        orient='tight')
    city_quantiles = dict(city_quantiles['data'])
    ## add road_area_quantile 
    city_quantiles['road_area'] = 0.30 * 50
    city_quantiles['affluence'] = 3.84
    return city_quantiles


def pois_count_for_isochrone(isochrone_polygon):
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
        select * from datasets_prep.bng_residential_projects A
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
        (avg(affluence_index)*4)/5 as total,
        0 as high_voted_gt_50
    from (
        select affluence_index from "hyperlocal_analysis_ind_dev"."ind_geoid_indices" A
        where st_intersects(
               st_geometryfromtext(geometry),
                st_geometryfromtext('""" + isochrone_polygon.wkt + """')
            )
            and type = '1km_grid'
        )  """

    all_pois_counts = wr.athena.read_sql_query(query_for_pois_in_isochrone, database='hyperlocal_analysis_ind_dev',
                                               ctas_approach=False)
    all_pois_counts = all_pois_counts[all_pois_counts['category'] != 'park']
    park_counts = wr.athena.read_sql_query(parks_data_isochrone_query, database='hyperlocal_analysis_ind_dev',
                                           ctas_approach=False)
    road_area_counts = wr.athena.read_sql_query(road_area_grided_query, database='hyperlocal_analysis_ind_dev',
                                                ctas_approach=False)
    aparments_counts = wr.athena.read_sql_query(aparments_data_isochrone_query, database='hyperlocal_analysis_ind_dev',
                                                ctas_approach=False)
    affluence_query = wr.athena.read_sql_query(affluence_query, database='hyperlocal_analysis_ind_dev',
                                               ctas_approach=False)
    # all_pois_counts = all_pois_counts.append(park_counts).append(road_area_counts).append(aparments_counts).append(affluence_query)
    all_pois_counts = pd.concat([all_pois_counts, park_counts, road_area_counts, aparments_counts, affluence_query])

    all_category_wise_count_needed_pair = get_categories_and_count_type_needed()
    all_pois_counts['count_needed'] = all_pois_counts[['category', 'branded', 'verified', 'total']].apply(
        lambda x: all_category_wise_count_needed_pair[x['category']] if x[
                                                                            'category'] in all_category_wise_count_needed_pair.keys() else None,
        axis=1)
    all_pois_counts['count'] = all_pois_counts[['category', 'branded', 'verified', 'total', 'count_needed']].apply(
        lambda x: x[x['count_needed']] if x['count_needed'] is not None else None, axis=1)
    all_pois_counts = all_pois_counts[all_pois_counts['count_needed'].notnull()][['category', 'count', ]]
    all_pois_counts = all_pois_counts.set_index('category').to_dict()['count']
    return all_pois_counts


def create_index_from_city_isochrone_counts(isochrone_counts, city_counts, isochrone_area, city_grid_level,
                                            total_area_at_this_level):
    category_indexs = {}
    for key in city_counts:
        if key not in isochrone_counts.keys():
            continue
        city_val = city_counts[key] * isochrone_area / total_area_at_this_level if key not in ['road_area',
                                                                                               'affluence'] else \
            city_counts[key]
        isochrone_val = isochrone_counts[key]
        if int(city_val) in [0, 1] and int(isochrone_val) == 0:
            category_indexs[key] = 0
            continue
        if isochrone_val <= 1 or city_val <= 1:
            isochrone_val += 2
            city_val += 2

        if isochrone_val > city_val:
            category_indexs[key] = 5
        else:
            category_indexs[key] = round((100 * (np.log(isochrone_val) / np.log(city_val))) / 20, 2)

    return category_indexs


def load_city_count(city_id, level, quantile=0.99):
    base_path = "revenue_score/cache_data_for_city_grids/"
    os.makedirs(base_path, exist_ok=True)
    if os.path.exists(f"{base_path}city_count_{city_id}_{level}.pkl"):
        city_count = pickle.load(open(f"{base_path}city_count_{city_id}_{level}.pkl", "rb"))
    else:
        city_count = get_city_grid_level_counts(city_id, level, quantile)
        pickle.dump(city_count, open(f"{base_path}city_count_{city_id}_{level}.pkl", "wb"))

    return city_count, 0.35 * 4 ** level


def get_isochrone_and_area(lat, lng, cost, type='driving', cost_type='time'):
    isochrone_polygon = get_isochrone(lat, lng, type, cost_type, cost)
    return isochrone_polygon, geom_area_in_sqkm(isochrone_polygon)


def create_grouped_indexes_from_indexs(indexs):
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
        }
    }

    all_category_wise_count_needed_pair = []
    for key, value in category_wise_count_needed_weighted.items():
        for k, v in value.items():
            all_category_wise_count_needed_pair.append((key, k, v, indexs.get(k, -1)))
    all_category_wise_count_needed_pair_df = pd.DataFrame(all_category_wise_count_needed_pair,
                                                          columns=['index_type', 'category', 'weight', 'index'])
    all_category_wise_count_needed_pair_df['weighted_index'] = all_category_wise_count_needed_pair_df['index'] * \
                                                               all_category_wise_count_needed_pair_df['weight']
    # print(all_category_wise_count_needed_pair_df[all_category_wise_count_needed_pair_df['index'] == -1])
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


def generate_revenue_score(lat, lng, travel_mode, cost_type, cost):
    isochrone_polygon, isochrone_area = get_isochrone_and_area(lat, lng, type=travel_mode, cost_type=cost_type,
                                                               cost=cost)

    grid_level_to_fetch = get_level_for_n_grids(isochrone_area / 0.35)
    city_counts, total_area_at_this_level = load_city_count(3, grid_level_to_fetch)

    poi_counts_isochrone = pois_count_for_isochrone(isochrone_polygon)
    indexs = create_index_from_city_isochrone_counts(poi_counts_isochrone, city_counts, isochrone_area,
                                                     grid_level_to_fetch,
                                                     total_area_at_this_level)
    indexs_df = pd.DataFrame.from_dict(indexs, orient='index').reset_index()
    indexs_df.columns = ['category', 'index']

    isochrone_poi_counts_df = pd.DataFrame.from_dict(poi_counts_isochrone, orient='index').reset_index()
    isochrone_poi_counts_df.columns = ['category', 'isochrone_counts']

    city_poi_counts_df = pd.DataFrame.from_dict(city_counts, orient='index').reset_index()
    city_poi_counts_df.columns = ['category', 'city_counts']

    all_merged = indexs_df.merge(isochrone_poi_counts_df, on='category', how='left').merge(city_poi_counts_df,
                                                                                           on='category', how='left')
    grouped_indexs = create_grouped_indexes_from_indexs(indexs)

    weights_dict = {
        'affluence_index': 0.19,
        'apartments_index': 0.19,
        'fashion_index': 0.19,
        'vibrancy': 0.14,
        'healthcare_index': 0.10,
        'company_index': 0.10,
        'malls_index': 0.05,
        'supermarket_index': 0.04,
    }

    revenue_score = create_revenue_score_from_indexs(grouped_indexs, weights_dict) * 20
    return revenue_score, grouped_indexs


if __name__ == '__main__':
    lat = 13.0552603
    lng = 77.7638939
    name = 'Orion Avenue'
    travel_mode = 'driving'
    cost_type = 'time'
    cost = 15
    revenue_score, grouped_indexs = generate_revenue_score(lat, lng, travel_mode, cost_type, cost)
    print(revenue_score, grouped_indexs)
