import sqlite3
import time

import shapely
import json
import pathlib
from hashlib import md5
import pandas as pd
import uuid
import revenue_score as R


def cluster_name_locality(func):
    def wrapper(*args, **kwargs):
        name_locality = func(*args, **kwargs)
        cluster_name = ",".join(list(filter(lambda x: x is not None, name_locality)))
        locality = name_locality[1]
        return cluster_name, locality

    return wrapper


@cluster_name_locality
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


def connect_to_db():
    conn = sqlite3.connect('/home/jyotiraditya/PycharmProjects/SiteReports/site_reports_v2.db')
    cur = conn.cursor()
    query = '''
    CREATE TABLE IF NOT EXISTS ind_site_reports_v2 (
    report_id TEXT,
    id TEXT,
    site_name TEXT,
    lat REAL,
    lng REAL,
    geometry TEXT,
    location varchar(255),
    catchment_type TEXT,
    top_brands TEXT,
    pois TEXT,
    projects TEXT,
    apartments TEXT,
    median_price TEXT,
    household_distribution TEXT,
    competition TEXT,
    population TEXT,
    affluence REAL,
    avg_cost_for_two REAL,
    revenue_score REAL,
    high_streets TEXT,
    shopping_malls TEXT,
    poi_counts TEXT,
    projects_counts TEXT,
    created_at INTEGER
);

    '''
    cur.execute(query)
    return conn, cur


def get_project_info(report_id=None, id=None):
    if report_id and id:
        query = """select * from ind_site_reports_v2 where report_id =:report_id and id=:id"""
    elif report_id:
        query = """select * from ind_site_reports_v2 where report_id =:report_id """
    elif id:
        query = """select * from ind_site_reports_v2 where  id=:id"""
    else:
        query = """select * from ind_site_reports_v2 """

    con, cur = connect_to_db()
    cur.execute(query, dict(report_id=report_id, id=id))
    cols = [col[0] for col in cur.description]
    data = cur.fetchall()
    cur.close()
    con.close()
    stores = pd.DataFrame(data=data, columns=cols)
    return stores


def get_all_reports():
    query = """select * from ind_site_reports_v2 """
    con, cur = connect_to_db()
    cur.execute(query)
    cols = [col[0] for col in cur.description]
    data = cur.fetchall()
    cur.close()
    con.close()
    stores = pd.DataFrame(data=data, columns=cols)
    return stores


# @retry(exceptions=(ValueError,),tries=5,delay=10)
def get_isochrone(lat, lng, travel_mode, cost_type, cost,
                  key="pk.eyJ1IjoibGFpcjA4MjYiLCJhIjoiY2tkcGoxcnRzMDZvODJxbXk0MWhlcWN2aSJ9.5-yjt_SUq4w5JII7CvD4cA"):
    print(lat, lng, travel_mode, cost_type, cost, key)
    import requests, logging, traceback
    pathlib.Path('cache').mkdir(exist_ok=True, parents=True)
    MAPBOX_ACCESS_TOKEN = key
    isochrone_id = 'i'
    try:
        if cost_type == "time":
            cost_type = "contours_minutes"
            isochrone_id = f"i{cost}min{travel_mode[0]}"
        elif cost_type == "distance":
            cost_type = "contours_meters"
            isochrone_id = f"i{cost}mt{travel_mode[0]}"
        else:
            raise Exception("cost_type must be either time or distance")

        if travel_mode not in ["driving", "walking", "cycling"]:
            raise Exception("travel_mode must be either driving or walking or cycling")

        mapbox_url = f'https://api.mapbox.com/isochrone/v1/mapbox/{travel_mode}/{lng}%2C{lat}?{cost_type}={cost}&polygons=true&denoise=1&access_token={MAPBOX_ACCESS_TOKEN}'
        # logging.debug(mapbox_url)
        url_md5 = md5(mapbox_url.encode()).hexdigest()
        filename = pathlib.Path('cache') / f'{url_md5}.json'
        if filename.exists():
            with open(filename) as file:
                # print('cache hit')
                data = json.load(file)
        else:
            response = requests.get(mapbox_url)
            if response.status_code == 200:
                data = response.json()
                with open(filename, 'w') as file:
                    json.dump(data, file)
            else:
                raise Exception("Mapbox API call failed with message: " + response.text)
        data = data["features"][0]["geometry"]["coordinates"][0]

        isochrone_polygon = []
        for point in data:
            isochrone_polygon.append(shapely.Point(point[0], point[1]))
        isochrone_polygon = shapely.Polygon(isochrone_polygon)

        return isochrone_polygon, isochrone_id
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())

        raise ValueError("Failed to fetch Isochrone Polygon")


def buffer_a_polygon_in_meters(polygon, meters):
    import pyproj
    from shapely.ops import transform

    wgs84 = pyproj.CRS('EPSG:4326')
    utm = pyproj.CRS('EPSG:32618')
    project_1 = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform
    project_2 = pyproj.Transformer.from_crs(utm, wgs84, always_xy=True).transform

    if isinstance(polygon, str):
        polygon = shapely.from_wkt(polygon)
    if meters > 0:
        polygon = transform(project_1, polygon)
        polygon = polygon.buffer(meters)
        polygon = transform(project_2, polygon)
    return polygon


def create_project(lat, lng, name, travel_mode, cost_type, cost):
    conn, cur = connect_to_db()
    if cost_type == 'buffer':
        isochrone_id = f'i{cost}mtb'
        isochrone_polygon = buffer_a_polygon_in_meters(shapely.Point(lng, lat), cost).wkt
        revenue_score = None
        weights = {}
    else:
        isochrone_polygon, isochrone_id = get_isochrone(lat, lng, travel_mode=travel_mode, cost_type=cost_type,
                                                        cost=cost)
        isochrone_polygon = isochrone_polygon.wkt
        revenue_score, weights = R.generate_revenue_score(lat, lng, travel_mode, cost_type, cost)
    location = get_street_name_locality(lat, lng)[0]
    # google_sheet_id = create_report_from_template(f'Site Report - {name.title()}')
    id = f"{int(lat * 10000)}_{int(lng * 10000)}"
    report_id = str(uuid.uuid4())
    site_name = f"{name.title()} - {location}"
    query_params = dict(site_name=site_name, lat=lat, lng=lng, isochrone_polygon=isochrone_polygon,
                        location=location, isochrone_id=isochrone_id, id=id, report_id=report_id,
                        created_at=int(time.time()), revenue_score=revenue_score, grouped_indexes=json.dumps(weights))
    query = '''INSERT INTO ind_site_reports_v2 (
    report_id,
    id,
    site_name,
    lat,
    lng,
    location,
    catchment_type,
    geometry,
    created_at,
    revenue_score,
    grouped_indexes
) VALUES (
    :report_id,
    :id,
    :site_name,
    :lat,
    :lng,
    :location,
    :isochrone_id,
    :isochrone_polygon,
    :created_at,
    :revenue_score,
    :grouped_indexes
) returning report_id,id;
'''
    cur.execute(query, query_params)
    project_id = (zip(('report_id', 'id'), cur.fetchone()))
    conn.commit()
    cur.close()
    conn.close()
    return {'project_id': project_id}


if __name__ == '__main__':
    # lat, lng = 13.063170, 77.620569
    lat, lng = 12.887373, 77.596901
    configs = ({"lat": lat, "lng": lng, "name": "Cultfit", "travel_mode": "driving", "cost_type": "time", "cost": 15},
               {"lat": lat, "lng": lng, "name": "Cultfit", "travel_mode": "driving", "cost_type": "buffer",
                "cost": 500},
               {"lat": lat, "lng": lng, "name": "Cultfit", "travel_mode": "driving", "cost_type": "buffer",
                "cost": 1000}
               )
    for i in configs:
        create_project(**i)
