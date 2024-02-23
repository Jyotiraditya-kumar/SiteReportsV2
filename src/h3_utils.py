import h3
import shapely
import json
import pandas as pd
from typing import List


def get_polygons(polygon):
    polygons = []
    if isinstance(polygon, str):
        polygon = shapely.from_wkt(polygon)
    if isinstance(polygon, shapely.MultiPolygon):
        polygons.extend(list(polygon.geoms))
    elif isinstance(polygon, shapely.Polygon):
        polygons.append(polygon)
    else:
        raise Exception(f"Expected a shapely polygon got {type(polygon)}")
    return polygons


def flatten_polygon(polygon):
    if isinstance(polygon, str):
        polygon = shapely.from_wkt(polygon)
    poly2d = shapely.Polygon([xy[0:2] for xy in list(polygon.exterior.coords)])
    return poly2d


def get_h3_cell_info(cell):
    d = {}
    lat, lon = h3.h3_to_geo(cell)
    d['lat'] = lat
    d['lon'] = lon
    d['h3_index'] = str(cell)
    res = h3.h3_get_resolution(cell)
    d['radius'] = h3.edge_length(res)
    d['poly']=shapely.Polygon(h3.h3_to_geo_boundary(cell,geo_json=True))
    return d


def get_h3_indexes_from_polygon(polygon: shapely.Polygon | shapely.MultiPolygon, level):
    polygons = get_polygons(polygon)
    h3_indexes = []
    if isinstance(polygons, list):
        for poly in polygons:
            poly_ = flatten_polygon(poly)
            h3_indexes.extend(list(h3.polyfill_geojson(shapely.geometry.mapping(poly_), level)))
    data = list(map(get_h3_cell_info, h3_indexes))
    df = pd.DataFrame(data)
    return df


def get_city_polygon(city_name, filename):
    if filename is None:
        df = pd.read_csv('/media/jyotiraditya/Ultra Touch/repos/SiteReports/src/ind_top_cities_geometry.csv')
    else:
        df = pd.read_csv(filename)
    city_poly: shapely.Polygon = shapely.from_wkt(
        df[df['name'].str.lower() == city_name.lower()]['geometry'].values.tolist()[0])
    return city_poly


def get_h3_index_for_city(city_name, h3_res, filename=None):
    poly = get_city_polygon(city_name, filename)
    return get_h3_indexes_from_polygon(poly, h3_res)


if __name__ == "__main__":
    df = pd.read_csv('/home/jyotiraditya/PycharmProjects/jupyterProject/notebooks/data/top_8_cities.csv')
    df['geometry'] = df['WKT'].apply(flatten_polygon)
    df=df.drop(columns=['WKT'])
    df.to_csv('/home/jyotiraditya/PycharmProjects/jupyterProject/notebooks/data/top_10_cities_2d.csv')
