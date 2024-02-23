from concurrent.futures import ProcessPoolExecutor

import requests
import io
import boto3
import time
import pandas as pd

class QueryAthena:

    def __init__(self, query, database):
        self.database = database
        self.folder = 'athena_results'
        self.bucket = 'tuzomldev'
        self.s3_input = 's3://' + self.bucket + '/' + self.folder
        self.s3_output = 's3://' + self.bucket + '/' + self.folder
        self.region_name = 'us-east-2'
        self.query = query

    def load_conf(self, q):
        try:
            self.client = boto3.client('athena', region_name=self.region_name, )
            response = self.client.start_query_execution(
                QueryString=q,
                QueryExecutionContext={
                    'Database': self.database
                },
                ResultConfiguration={
                    'OutputLocation': self.s3_output,
                }
            )
            self.filename = response['QueryExecutionId']
            print('Execution ID: ' + response['QueryExecutionId'])
            return response

        except Exception as e:
            print(e)

    def run_query(self):
        queries = [self.query]
        for q in queries:
            res = self.load_conf(q)
        try:
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                query_status = \
                    self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution'][
                        'Status'][
                        'State']
                print(query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    raise Exception('Athena query with the string "{}" failed or was cancelled'.format(self.query))
                if query_status != 'SUCCEEDED' :
                    time.sleep(1)
            print('Query "{}" finished.'.format(self.query))

            df = self.obtain_data()
            print("Data Retrieved from S3")
            return df

        except Exception as e:
            print(e)

    def obtain_data(self):
        try:
            self.resource = boto3.resource('s3', region_name=self.region_name, )

            response = self.resource \
                .Bucket(self.bucket) \
                .Object(key=self.folder + "/" + self.filename + '.csv') \
                .get()

            return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')
        except Exception as e:
            print(e)


import urllib.parse


def execute_athena_query_lambda(query, database):
    lambda_url = "https://iegirpm6rdw72ozprrnkialdcm0fqgtn.lambda-url.us-east-2.on.aws/"
    print(query)
    url = f"{lambda_url}?query={urllib.parse.quote(query)}&database={database}"
    resp = requests.get(url)
    print(url)
    print(resp.text)
    df = pd.read_csv(io.BytesIO(resp.content), encoding='utf8')
    return df


class DataExtraction:
    def __init__(self, poly):
        self.poly = poly

    def get_location_poi_data(self):
        query = f"""
        select d.id,
            d.name,
            lat,
            lng,
            brand_id,
            brand_name,
            "number_of_votes",rating,
            category,top_category,type, 
            coalesce(num_reviews_per_day,0) as reviews_per_day,source
        from ind_poi_data_v2_gold d where brand_id<>'N_A' and category in ('clothing_store', 'shoe_store', 'restaurant', 'home_decor', 'jewelry_store', 'coffee_shop', 'supermarket',
             'electronic_store', 'cosmetic', 'gym_fitness') and st_contains(st_geometryfromtext('{self.poly}'),st_point(lng,lat))
            """
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        df = df.drop_duplicates(subset=['id', 'source'])
        return df

    def get_competitors_data(self, competitor_brand_ids, anchor_brand_ids, *args, **kwargs):
        query = f"""
        select d.id,
            d.name,
            lat,
            lng,
            brand_id,
            brand_name,
            "number_of_votes",rating,
            category,top_category,type, 
            coalesce(num_reviews_per_day,0) as reviews_per_day,source,
            case when brand_id in {tuple(competitor_brand_ids)} then 'primary_competitor'
                when brand_id in {tuple(anchor_brand_ids)} then 'anchor_competitor' end as competitor_type
        from ind_poi_data_v2_gold d where brand_id in {tuple(competitor_brand_ids + anchor_brand_ids)} and st_contains(st_geometryfromtext('{self.poly}'),st_point(lng,lat))
            """
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        df = df.drop_duplicates(subset=['id', 'source'])
        return df

    def get_location_properties(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        SELECT g.* FROM "ind_residential_properties_gold" g join poly p on st_contains(st_geometryfromtext(p.geometry),st_point(g.lng,g.lat)) where property_type='Apartment' and buy_rent in ('Buy','Rent')"""
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        df = df.drop_duplicates(subset=['id', 'source'])
        return df

    def get_location_population_data(self):
        query = f"""
        SELECT geoid,total_population,"age_0_19",age_20_34,age_35_59,"age_60+",geometry FROM hyperlocal_analysis_ind_prod.ind_geoid_demographics where geometry_type='1km_grid'  
        and st_intersects(st_geometryfromtext('{self.poly}'),st_geometryfromtext(geometry))
    """
        # query=f"""select 
        #     sum(total_population * intersection) as total_population,
        #     sum(age_0_19 * intersection) as age_0_19,
        # 	sum(age_20_34 * intersection) as age_20_34,
        # 	sum(age_35_59 * intersection) as age_35_59,
        # 	sum("age_60+" * intersection) as "age_60+"
        # from (
        # 		SELECT geoid,
        # 			total_population,
        # 			"age_0_19",
        # 			age_20_34,
        # 			age_35_59,
        # 			"age_60+",
        # 			st_area(
        # 				st_intersection(
        # 					st_geometryfromtext(geometry),
        # 					st_geometryfromtext('{self.poly}')
        # 				)
        # 			) / st_area(st_geometryfromtext(geometry)) as intersection
        # 		FROM hyperlocal_analysis_ind_prod.ind_geoid_demographics
        # 		where geometry_type = '1km_grid'
        # 	)
        # where intersection <> 0"""

        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df=wr.athena.read_sql_query(query,database='datasets_prep',ctas_approach=False)
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        # df['geometry'] = df['geometry'].astype(str)
        return df

    def get_location_projects_data(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        SELECT g.* FROM "ind_residential_projects_gold" g join poly p on st_contains(st_geometryfromtext(p.geometry),st_point(g.lng,g.lat))
    """
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        df = df.drop_duplicates(subset=['id', 'source'])
        return df

    def get_location_income_distribution_data(self):
        query = f"""
        SELECT ews_hh,lig_hh,mig_hh,aig_hh,eig_hh,avg_household_income,geom FROM "datasets_prep"."population_income_distribution" where st_intersects(st_geometryfromtext('{self.poly}'),st_geometryfromtext(geom))
    """
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        df['geom'] = df['geom'].astype(str)
        return df

    def get_location_affluence_data(self):
        query = f"""
        with poly as (select '{self.poly}' as geometry)
        select geoid,affluence_index from datasets_prep.ind_1km_grid_affluence  d join poly p on st_intersects(st_geometryfromtext(d.geometry),st_geometryfromtext(p.geometry)) where geometry_type='1km_grid'
    
    """
        data_dir = self.data_dir
        data_dir.mkdir(parents=True, exist_ok=True)
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        df.to_csv(data_dir / "location_affluence_data.csv", index=False)

    def get_avg_cft(self):
        query = f"""
        select avg(cast(additional_data['cost_for_two'] as double)) as avg_cost_for_two from zomato_restaurants_silver where st_contains(st_geometryfromtext('{self.poly}'),st_point(lng,lat))
    """
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        avg_cft = df['avg_cost_for_two'][0]
        return avg_cft

    def get_malls(self):
        query = f"""
        select * from datasets_prep.bng_malls where st_intersects(st_geometryfromtext('{self.poly}'),st_geometryfromtext(wkt))
    """
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        return df

    def get_high_streets(self):
        query = f"""
        select cluster_name,locality,polygon_updated,area,lat,lng from datasets_prep.bng_high_streets where st_intersects(st_geometryfromtext('{self.poly}'),st_geometryfromtext(polygon_updated))
    """
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')
        return df

    def get_demand_generators(self):
        query = f"""
        select category,
	count(*) as count
from ind_poi_data_v2_gold
where (
		category in ('college', 'hospital', 'clinic', 'pharmacy','clothing_store', 'shoe_store', 'restaurant', 'home_decor', 'jewelry_store', 'coffee_shop', 'supermarket',
             'electronic_store', 'cosmetic', 'gym_fitness')
		or (
			category in ('school')
			and type in ('pre_school', 'play_school', 'high_school', 'N_A')
		)
	)
	and st_contains(st_geometryfromtext('{self.poly}'), st_point(lng, lat))
group by category """
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')

        return df

    def get_companies_data(self):
        query = f"""
            select type,
                count(*) as count
                from ind_poi_data_v2_gold
                where top_category='company'
                and st_contains(st_geometryfromtext('{self.poly}'), st_point(lng, lat))
                group by type"""
        Query = QueryAthena(query, 'datasets_prep')
        df = Query.run_query()
        # df = execute_athena_query_lambda(query, 'datasets_prep')

        return df

    def execute_function(self, func):
        func()

    def extract_data(self):
        mthds = [
            # self.get_location_poi_data,
            # self.get_location_zomato_data,
            # self.get_location_population_data,
            self.get_location_projects_data,
            self.get_location_properties,
            # self.get_location_affluence_data
        ]
        with ProcessPoolExecutor(max_workers=10) as executor:
            res = executor.map(self.execute_function, mthds)
            for i in res:
                print(i)


# if __name__ == "__main__":
#     # pids = ['130110_775547', '130009_776325', '130552_777638', '130631_776205', '128873_775969', '129696_776307',
#     #         '130959_775791']
#     for pid in ['130110_775547']:
#         e = DataExtraction(pid)
#         e.extract_data()

if __name__ == "__main__":
    poly = 'POLYGON ((77.494736 13.050067, 77.493202 13.050027, 77.494661 13.049952, 77.494736 13.048977, 77.497472 13.048763, 77.497647 13.047937, 77.499736 13.04778, 77.500736 13.046834, 77.502736 13.046714, 77.503568 13.045858, 77.505192 13.045483, 77.505736 13.044704, 77.507736 13.044658, 77.50847 13.043761, 77.510038 13.043329, 77.510736 13.042533, 77.512985 13.042275, 77.513736 13.041339, 77.515795 13.041086, 77.515927 13.040217, 77.518037 13.038327, 77.518736 13.03803, 77.520001 13.038291, 77.520736 13.034614, 77.521736 13.035243, 77.522386 13.036378, 77.523358 13.036027, 77.522736 13.035156, 77.522101 13.035027, 77.52351 13.033027, 77.522656 13.032027, 77.523545 13.031027, 77.523529 13.030027, 77.522736 13.028768, 77.522486 13.027277, 77.521736 13.026951, 77.522736 13.026636, 77.523736 13.027137, 77.524025 13.028738, 77.524872 13.028891, 77.525622 13.030141, 77.527176 13.029588, 77.527848 13.030027, 77.527318 13.029445, 77.527635 13.027027, 77.528507 13.026027, 77.528407 13.025356, 77.527719 13.025027, 77.528115 13.024406, 77.528197 13.022566, 77.527736 13.021911, 77.526466 13.022297, 77.525024 13.022027, 77.525736 13.020642, 77.526868 13.020159, 77.528736 13.018097, 77.529736 13.017925, 77.530049 13.017027, 77.529736 13.015926, 77.52783 13.015027, 77.529024 13.014315, 77.529026 13.013027, 77.528157 13.011607, 77.525617 13.010146, 77.524657 13.009106, 77.524528 13.008027, 77.523522 13.007027, 77.524528 13.004819, 77.525736 13.005193, 77.526016 13.004027, 77.525312 13.003451, 77.525323 13.002027, 77.524459 13.001304, 77.52421 12.999553, 77.522716 12.998027, 77.522526 12.997027, 77.523483 12.995027, 77.524736 12.994358, 77.525729 12.993019, 77.529137 12.991027, 77.53106 12.988027, 77.531776 12.986027, 77.533736 12.984267, 77.534002 12.982762, 77.531796 12.982027, 77.532622 12.980912, 77.532716 12.980006, 77.533538 12.979828, 77.535394 12.977684, 77.537736 12.978145, 77.538736 12.977317, 77.540736 12.978651, 77.542411 12.977027, 77.541207 12.976556, 77.537627 12.973136, 77.537736 12.971502, 77.539534 12.972229, 77.540256 12.972027, 77.543128 12.969419, 77.544736 12.967169, 77.545736 12.967286, 77.546736 12.967945, 77.548736 12.970145, 77.549736 12.969679, 77.551736 12.969525, 77.554736 12.970356, 77.555736 12.971105, 77.556715 12.971006, 77.559198 12.968488, 77.560736 12.968001, 77.560894 12.969027, 77.56195 12.970027, 77.563646 12.969027, 77.563185 12.968578, 77.562943 12.967027, 77.563539 12.966829, 77.564736 12.967249, 77.565371 12.968392, 77.568263 12.969027, 77.567391 12.974027, 77.566686 12.975027, 77.568415 12.978027, 77.568283 12.981027, 77.568736 12.981354, 77.569736 12.98089, 77.570538 12.978027, 77.570736 12.975309, 77.571378 12.976027, 77.570904 12.976195, 77.570892 12.977027, 77.571471 12.979293, 77.572736 12.979365, 77.573736 12.980793, 77.574736 12.980201, 77.575736 12.980742, 77.578356 12.981027, 77.576546 12.981837, 77.576736 12.983093, 77.575617 12.983146, 77.574075 12.981689, 77.573451 12.981741, 77.573296 12.984467, 77.573736 12.984988, 77.574736 12.984101, 77.577736 12.986807, 77.579334 12.986625, 77.579957 12.986027, 77.579067 12.990027, 77.579765 12.990998, 77.579679 12.992027, 77.580736 12.992509, 77.581738 12.995025, 77.583013 12.996027, 77.582624 12.99714, 77.583821 12.997027, 77.584511 12.995027, 77.584312 12.993603, 77.584881 12.992883, 77.58571 12.993027, 77.58483 12.99312, 77.584979 12.995027, 77.585736 12.995962, 77.589615 12.996027, 77.588245 12.996536, 77.587045 12.998336, 77.585109 12.998399, 77.584454 13.000027, 77.584467 13.006296, 77.585457 13.013306, 77.587033 13.013323, 77.58858 13.011871, 77.589418 13.011709, 77.589736 13.010871, 77.590623 13.010914, 77.590736 13.009968, 77.590913 13.011027, 77.589977 13.011268, 77.589907 13.012027, 77.589042 13.012332, 77.588924 13.013027, 77.588055 13.013346, 77.58785 13.01414, 77.585115 13.015405, 77.584148 13.016439, 77.583946 13.021027, 77.58407 13.021693, 77.584872 13.022027, 77.584736 13.025253, 77.58435 13.024413, 77.583494 13.024027, 77.583254 13.020027, 77.582109 13.017655, 77.579736 13.017585, 77.578736 13.01669, 77.57756 13.017203, 77.576736 13.015817, 77.575736 13.017701, 77.575318 13.017445, 77.575009 13.015754, 77.573736 13.015614, 77.573321 13.016027, 77.573736 13.018218, 77.574352 13.019412, 77.575736 13.020521, 77.575736 13.021876, 77.575206 13.023027, 77.57647 13.024027, 77.576 13.025291, 77.575736 13.025672, 77.574736 13.025025, 77.574164 13.025455, 77.572407 13.025697, 77.572736 13.027552, 77.574935 13.028027, 77.573736 13.029276, 77.571736 13.029893, 77.570889 13.031179, 77.570147 13.031438, 77.569736 13.032551, 77.568837 13.033127, 77.568708 13.034027, 77.567457 13.035027, 77.567837 13.036027, 77.566418 13.036708, 77.564736 13.039322, 77.56383 13.038933, 77.5636 13.038027, 77.562736 13.037353, 77.561736 13.038403, 77.559785 13.039075, 77.558736 13.037744, 77.558193 13.039027, 77.558331 13.041027, 77.557736 13.041793, 77.556736 13.042145, 77.55623 13.038533, 77.554736 13.038371, 77.554149 13.039027, 77.55407 13.040027, 77.554868 13.041158, 77.553736 13.042632, 77.551445 13.040027, 77.550736 13.039773, 77.549909 13.040199, 77.548557 13.040027, 77.548952 13.039243, 77.548492 13.037272, 77.547736 13.036813, 77.546736 13.03695, 77.546263 13.0365, 77.546046 13.036027, 77.547107 13.033027, 77.547097 13.032027, 77.548085 13.031027, 77.546736 13.03081, 77.546316 13.031607, 77.544186 13.032476, 77.543736 13.033768, 77.542528 13.033236, 77.542149 13.032614, 77.541312 13.032603, 77.540962 13.034027, 77.541104 13.03466, 77.541902 13.035027, 77.541736 13.038327, 77.541261 13.037027, 77.539063 13.0347, 77.537378 13.035669, 77.537052 13.036711, 77.537434 13.038027, 77.536174 13.038465, 77.535736 13.03916, 77.534536 13.038228, 77.533174 13.038465, 77.532794 13.039085, 77.530736 13.039502, 77.530169 13.038594, 77.528736 13.037692, 77.526736 13.037823, 77.525736 13.03745, 77.525574 13.037864, 77.523736 13.03813, 77.523183 13.039027, 77.523816 13.039947, 77.523736 13.040475, 77.522736 13.040383, 77.521736 13.039718, 77.520503 13.039793, 77.51844 13.04173, 77.516736 13.042608, 77.514736 13.04279, 77.513736 13.043572, 77.511736 13.04342, 77.510974 13.044264, 77.509333 13.044624, 77.508736 13.045379, 77.506736 13.045411, 77.505942 13.046232, 77.504216 13.046506, 77.503736 13.047261, 77.501736 13.047217, 77.501099 13.047389, 77.500838 13.048128, 77.497975 13.048265, 77.497736 13.049135, 77.494864 13.049154, 77.494736 13.050067))'
    a = DataExtraction(poly)
    queries = {"cft": a.get_avg_cft, 'population': a.get_location_population_data,
               "demand_gen": a.get_demand_generators,
               "income_dist": a.get_location_income_distribution_data, "malls": a.get_malls,
               "companies": a.get_companies_data, "high_streets": a.get_high_streets}
    times_taken = []
    for i in range(5):
        time_taken = {}
        for query in queries:
            f = queries[query]
            start = time.time()
            data = f()
            end = time.time()
            time_taken[query] = int(end - start)
        print(time_taken)
        times_taken.append(time_taken)
    pd.DataFrame(times_taken).to_csv('times_taken.csv', mode='a')
