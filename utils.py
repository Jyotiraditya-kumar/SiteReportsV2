import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import shapely
import json
import pathlib
from hashlib import md5
import pandas as pd
import uuid
import revenue_score as R
import calculations_v5 as C


def cluster_name_locality(func):
    def wrapper(*args, **kwargs):
        name_locality = func(*args, **kwargs)
        cluster_name = ",".join(list(filter(lambda x: x is not None, name_locality)))
        locality = name_locality[1]
        return cluster_name, locality

    return wrapper


@cluster_name_locality
def get_street_name_locality(
    lat,
    lng,
    mapbox_key="pk.eyJ1IjoibGFpcjA4MjYiLCJhIjoiY2tkcGoxcnRzMDZvODJxbXk0MWhlcWN2aSJ9.5-yjt_SUq4w5JII7CvD4cA",
):
    import requests

    BASE_URL = f"https://api.mapbox.com/search/geocode/v6/reverse?longitude={lng}&latitude={lat}&access_token={mapbox_key}"
    try:
        response = requests.get(BASE_URL)
    except:
        return "", None
    data = response.json()
    if len(data["features"]) > 0:
        data_1 = data["features"][0]
        # print(data_1['properties']['context'])
        if "street" in data_1["properties"]["context"].keys():
            street_name = data_1["properties"]["context"]["street"]["name"]
        else:
            street_name = ""

        if "neighborhood" in data_1["properties"]["context"].keys():
            neighbourhood = (
                ", " + data_1["properties"]["context"]["neighborhood"]["name"]
            )
        else:
            neighbourhood = ""

        if "locality" in data_1["properties"]["context"].keys():
            locality = data_1["properties"]["context"]["locality"]["name"]
        else:
            locality = None
        return street_name + neighbourhood, locality
    else:
        return "", None


# @retry(exceptions=(ValueError,),tries=5,delay=10)
def get_isochrone(
    lat,
    lng,
    travel_mode,
    cost_type,
    cost,
    key="pk.eyJ1IjoibGFpcjA4MjYiLCJhIjoiY2tkcGoxcnRzMDZvODJxbXk0MWhlcWN2aSJ9.5-yjt_SUq4w5JII7CvD4cA",
):
    print(lat, lng, travel_mode, cost_type, cost, key)
    import requests, logging, traceback

    pathlib.Path("cache").mkdir(exist_ok=True, parents=True)
    MAPBOX_ACCESS_TOKEN = key
    isochrone_id = "i"
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

        mapbox_url = f"https://api.mapbox.com/isochrone/v1/mapbox/{travel_mode}/{lng}%2C{lat}?{cost_type}={cost}&polygons=true&denoise=1&access_token={MAPBOX_ACCESS_TOKEN}"
        # logging.debug(mapbox_url)
        url_md5 = md5(mapbox_url.encode()).hexdigest()
        filename = pathlib.Path("cache") / f"{url_md5}.json"
        if filename.exists():
            with open(filename) as file:
                # print('cache hit')
                data = json.load(file)
        else:
            response = requests.get(mapbox_url)
            if response.status_code == 200:
                data = response.json()
                with open(filename, "w") as file:
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

    wgs84 = pyproj.CRS("EPSG:4326")
    utm = pyproj.CRS("EPSG:32618")
    project_1 = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True).transform
    project_2 = pyproj.Transformer.from_crs(utm, wgs84, always_xy=True).transform

    if isinstance(polygon, str):
        polygon = shapely.from_wkt(polygon)
    if meters > 0:
        polygon = transform(project_1, polygon)
        polygon = polygon.buffer(meters)
        polygon = transform(project_2, polygon)
    return polygon


def execute_function(function, loc_info):
    loc_info_ = loc_info[0]
    kwargs = loc_info[1]
    return function(poly=loc_info_[0], lat=loc_info_[1], lng=loc_info_[2], **kwargs)


def execute_functions(functions, loc_info, *args, **kwargs):
    with ThreadPoolExecutor(max_workers=10) as executor:
        res = executor.map(
            execute_function,
            list(functions.values()),
            [[loc_info, kwargs] for _ in range(len(functions))],
        )
        res = list(res)
        return dict(zip(functions.keys(), res))


def get_initial_data(
    lat, lng, name, travel_mode, cost_type, cost, competitor_brand_ids, anchor_brand_ids
):
    if cost_type == "buffer":
        isochrone_id = f"i{cost}mtd"
        isochrone_polygon = buffer_a_polygon_in_meters(
            shapely.Point(lng, lat), cost
        ).wkt
    else:
        isochrone_polygon, isochrone_id = get_isochrone(
            lat, lng, travel_mode=travel_mode, cost_type=cost_type, cost=cost
        )
        isochrone_polygon = isochrone_polygon
    location = get_street_name_locality(lat, lng)[0]
    site_name = f"{name.title()} - {location}"
    funcs = dict(
        avg_cost_for_two=C.get_avg_cft,
        population=C.get_population_index,
        demand_generators=C.get_demand_generator_data,
        companies=C.get_companies,
        household_distribution=C.get_income_distribution,
        shopping_malls=C.get_malls,
        high_streets=C.get_high_streets,
        competition=C.get_competition,
    )
    resp = execute_functions(
        funcs,
        (
            isochrone_polygon,
            lat,
            lng,
        ),
        travel_mode=travel_mode,
        cost_type=cost_type,
        cost=cost,
        competitor_brand_ids=competitor_brand_ids,
        anchor_brand_ids=anchor_brand_ids,
    )
    return resp


def get_secondary_data(
    lat,
    lng,
    name,
    travel_mode,
    cost_type,
    cost,
    competitor_brand_ids,
    anchor_brand_ids,
    location_score_weights,
):
    if cost_type == "buffer":
        isochrone_id = f"i{cost}mtd"
        isochrone_polygon = buffer_a_polygon_in_meters(
            shapely.Point(lng, lat), cost
        ).wkt
    else:
        isochrone_polygon, isochrone_id = get_isochrone(
            lat, lng, travel_mode=travel_mode, cost_type=cost_type, cost=cost
        )
        isochrone_polygon = isochrone_polygon
    # location = get_street_name_locality(lat, lng)[0]
    # site_name = f"{name.title()} - {location}"
    funcs = dict(
        apartments=C.get_rent_and_sale_prices,
        projects=C.get_projects,
        pois=C.get_category_count,
        revenue_score=R.generate_revenue_score,
    )
    resp = execute_functions(
        funcs,
        (isochrone_polygon, lat, lng),
        travel_mode=travel_mode,
        cost_type=cost_type,
        cost=cost,
        catchment_type=isochrone_id,
        location_score_weights=location_score_weights,
        competitor_brand_ids=competitor_brand_ids,
        anchor_brand_ids=anchor_brand_ids,
    )
    return resp
