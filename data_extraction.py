from concurrent.futures import ProcessPoolExecutor

import awswrangler as wr
from pathlib import Path

import h3_utils
import utils as U
import shapely


class DataExtraction:
    def __init__(self, site_id, location_type='location'):
        self.site_id = site_id
        self.site_info = U.get_project_info(id=site_id)
        geometries = self.site_info['geometry'].tolist()
        geometries = [shapely.from_wkt(g) for g in geometries]
        if location_type == 'location':
            self.poly = shapely.unary_union(geometries).wkt
            self.data_dir = Path(f"data/projects_data/{self.site_id}/raw")
        elif location_type == 'city':
            self.poly = h3_utils.get_city_polygon(city_name=site_id, filename=None).wkt
            self.data_dir = Path(f"data/common_data/{self.site_id}/raw")

    def get_location_poi_data(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        select d.id,
            d.name,
            lat,
            lng,
            case
                when source = 'gmaps_v2' then additional_data [ 'hexadecimal_cid' ]
                when source = 'zomato' then id else null
            end as cid,
            brand_id,
            brand_name,
            "number_of_votes",rating,
            additional_data [ 'place_url' ] place_url,
            source,
            category,top_category,type,
            created_at
        from ind_poi_data_v2_gold_creation_v3 d
            join poly p on st_contains(
                st_geometryfromtext(geometry),
                st_point(lng, lat)
                )"""
        data_dir = self.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        df = wr.athena.read_sql_query(query, database='datasets_prep')
        df = df.drop_duplicates(subset=['id', 'source'])
        df.to_csv(data_dir / "location_poi_data.csv", index=False)

    def get_location_properties(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        SELECT g.* FROM "bng_residential_properties" g join poly p on st_contains(st_geometryfromtext(p.geometry),st_point(g.lng,g.lat))"""
        data_dir = self.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        df = wr.athena.read_sql_query(query, database='datasets_prep')
        df = df.drop_duplicates(subset=['id', 'source'])
        df.to_csv(data_dir / "location_properties_data.csv", index=False)

    def get_location_population_data(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        ,grid as (
    SELECT g.x_pixel,g.y_pixel,g.geometry FROM "hyperlocal_analysis_ind_prod"."ind_100m_grid_geometry" g join poly p on st_intersects(st_geometryfromtext(g.geometry),st_geometryfromtext(p.geometry)))
    select p.*,g.geometry from grid g join hyperlocal_analysis_ind_dev.ind_100m_grid_age_sex_pop p on g.x_pixel=p.x_pixel and g.y_pixel=p.y_pixel
    """
        data_dir = self.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        df = wr.athena.read_sql_query(query, database='datasets_prep')
        df.to_csv(data_dir / "location_population_data.csv", index=False)

    def get_location_projects_data(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        SELECT g.* FROM "bng_residential_projects" g join poly p on st_contains(st_geometryfromtext(p.geometry),st_point(g.lng,g.lat))
    """
        data_dir = self.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        df = wr.athena.read_sql_query(query, database='datasets_prep')
        df = df.drop_duplicates(subset=['id', 'source'])
        df.to_csv(data_dir / "location_projects_data.csv", index=False)

    def get_location_affluence_data(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        select geoid,affluence_index from datasets_prep.ind_1km_grid_affluence  d join poly p on st_intersects(st_geometryfromtext(d.geometry),st_geometryfromtext(p.geometry)) where geometry_type='1km_grid'
    
    """
        data_dir = self.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        df = wr.athena.read_sql_query(query, database='datasets_prep')
        df.to_csv(data_dir / "location_affluence_data.csv", index=False)

    def get_location_zomato_data(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        select d.id,name,lat,lng,brand_name,brand_id,number_of_votes,rating,additional_data['cost_for_two'] as cost_for_two from zomato_restaurants_silver d join poly p on st_contains(st_geometryfromtext(geometry),st_point(lng,lat))
    """
        data_dir = self.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        df = wr.athena.read_sql_query(query, database='datasets_prep')
        df = df.drop_duplicates(subset='id')
        df.to_csv(data_dir / "location_zomato_data.csv", index=False)

    def get_location_household_data(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        ,grid as (
    SELECT g.* FROM ind_grid_household_data g join poly p on st_intersects(st_geometryfromtext(g.geometry),st_geometryfromtext(p.geometry)))
    select * from grid
    """
        data_dir = self.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        df = wr.athena.read_sql_query(query, database='datasets_prep')
        df.to_csv(data_dir / "location_household_data.csv", index=False)

    def execute_function(self, func):
        func()

    def extract_data(self):
        mthds = [
            # self.get_location_household_data(),
            self.get_location_poi_data,
            # self.get_location_zomato_data,
            # self.get_location_population_data,
            # self.get_location_projects_data,
            # self.get_location_properties,
            # self.get_location_affluence_data
        ]
        with ProcessPoolExecutor(max_workers=10) as executor:
            executor.map(self.execute_function, mthds)


if __name__ == "__main__":
    pids = ['130110_775547', '130009_776325', '130552_777638']
    for pid in pids:  # ['130631_776205', '128873_775969']:
        e = DataExtraction(pid)
        e.extract_data()

# if __name__ == "__main__":
#     for pid in ['bangalore']:
#         e = DataExtraction(pid, location_type='city')
#         e.get_location_poi_data()
