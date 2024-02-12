import json
import time

from src import sheets_api
import requests
import pandas as pd
import utils as U

manager = sheets_api.GoogleSheetsManager()


def create_report_from_template(title):
    template_id = "12CbqlizKSN7r66hpytciM_s6McKjsbiJeuNejMaDjpU"
    new_sh = manager.client.copy(file_id=template_id, title=title, folder_id=folder_ids[env])
    return new_sh.id, new_sh.url


def get_sheet_info(site_id):
    query = """select * from site_reports_google_sheets where site_id = :site_id """
    con, cur = U.connect_to_db()
    cur.execute(query, dict(site_id=site_id))
    cols = [col[0] for col in cur.description]
    data = cur.fetchall()
    try:
        sheet_id = pd.DataFrame(data=data, columns=cols).dropna(subset=[sheet_cols[env]]).to_dict('records')[0][sheet_cols[env]]
    except IndexError:
        sheet_id, _ = create_report_from_template(site_id)
        query = f"""insert into site_reports_google_sheets (site_id, {sheet_cols[env]}) values (:site_id, :sheet_id)"""
        cur.execute(query, dict(site_id=site_id, sheet_id=sheet_id))
        con.commit()
    finally:
        cur.close()
        con.close()
    return sheet_id


class Dashboard:
    def __init__(self, proj_id):
        self.proj_id = proj_id
        self.project_info = U.get_project_info(id=proj_id)
        self.data = self.project_info.query("catchment_type!='i1000mtb'").to_dict('records')
        self.data = [{k: json.loads(v) if "{" in str(v) or "[" in str(v) else v for k, v in project.items()} for project
                     in
                     self.data]
        self.data = {project['catchment_type']: project for project in self.data}
        self.data_ = dict(data=self.data.copy())
        self.data = self.data_.copy()
        # self.project_info = dict(google_sheet_id='1-cD1O9393HZYv52q4M1pPPohqdSVCv6MlrEhEz-BhsQ')
        # self.sheet, self.sheet_url = create_report_from_template(self.data['data']['i15mind']['site_name'])
        self.project_info = dict(google_sheet_id=get_sheet_info(proj_id))
        self.sheet = self.get_google_sheet()
        self.sheet.update_title(self.data['data']['i15mind']['site_name'])
        self.report_worksheet = self.sheet.get_worksheet(0)
        self.existing_worksheets = {sheet['properties']['title']: sheet['properties']['index'] for sheet in
                                    self.sheet.fetch_sheet_metadata()['sheets']
                                    }
        self.worksheets = {
            'projects': 1,
            'pois_ranked': 2,
            'competition': 3,
            'high_streets': 4,
            'shopping_malls': 5,
            'apartments': 6,
            "top_brands": 7
        }

    def create_data_worksheets(self):
        for sheet_title in self.worksheets.keys():
            if sheet_title not in self.existing_worksheets:
                manager.create_new_worksheet(self.sheet, sheet_title)

    def get_google_sheet(self):
        sheet = manager.get_sheet_by_id(self.project_info['google_sheet_id'])
        return sheet

    def clear_google_sheet(self):
        ranges_to_clear = []
        with open("dashboard_copy.json") as f:
            data = json.load(f)
        for i in data:
            ranges_to_clear.append(i['range'])
        ranges_to_clear += ["E85:K92", 'E107:I110', 'E62:K66', 'E121:K124', 'N18:O35', 'E74:H79', 'E99:H103', 'F5',
                            'F17']
        self.report_worksheet.batch_clear(ranges_to_clear)

    def generate_report_data(self):
        eps = ["cft", "population", "companies", "education", "projects", "affluence", "income", "competition",
               "category_count", "property_price", "medical", "high_street", ]
        all_data = {"data": {}}
        for ep in eps:
            url = f"http://127.0.0.1:8000/site_report/{self.proj_id}/{ep}"
            req = requests.get(url)
            try:
                resp = req.json()
            except Exception as e:
                print(url, e)
                raise e
            data = resp['data']
            all_data['data'][ep] = data
        self.data = all_data

    def populate_single_cells(self):
        with open("dashboard_copy.json") as f:
            data = json.load(f)
        for i in data:
            try:
                i['values'] = [[self.get_dictionary_value(self.data, i['values'][0][0])]]
            except Exception as e:
                print(e, i['values'][0][0])
                i['values'] = [[None]]
        self.report_worksheet.batch_update(data)

    def insert_pois_ranked(self):
        a = pd.DataFrame(self.data['data']['i15mind']['pois']['data'])['top_pois']
        from functools import reduce
        df = pd.DataFrame(reduce(lambda a, b: a + b, a.values.tolist())).sort_values(by='reviews_per_day',
                                                                                     ascending=False)
        manager.write_dataframe_to_worksheet(df, self.sheet.get_worksheet(self.worksheets['pois_ranked']))

    def update_top_brands(self):
        df_ = pd.DataFrame(self.data['data']['i15mind']['top_brands']).sort_values(by='reviews_per_day',
                                                                                    ascending=False)
        df = df_.copy()
        cols = {"brand_name": "Brand", "category": "Category",
                "% of Catchment visitors": "% of Catchment visitors", "distance": "Distance from Site",
                }
        for k, v in cols.items():
            if k not in df.columns.tolist():
                df[k] = None
        df.rename(columns=cols, inplace=True)
        df = df[cols.values()]
        df['Distance from Site'] = df['Distance from Site'].apply(lambda x: f"{x} km")
        manager.write_dataframe_to_worksheet(df_, self.sheet.get_worksheet(self.worksheets['top_brands']))
        self.report_worksheet.batch_update([{"range": "E74:H78", "values": df.head(5).values.tolist()}])

    def update_category_count(self):
        buff = pd.DataFrame(self.data['data']['i500mtd']['pois']['data'])[['category', 'count']]
        cat = pd.DataFrame(self.data['data']['i15mind']['pois']['data'])[['category', 'count']]
        cols = {"category": "Category", "count": "Count", "Percentage": "Percentage", "Top Brands": "Top Brands"}
        for comp_df in [buff, cat]:
            for k, v in cols.items():
                if k not in comp_df.columns.tolist():
                    comp_df[k] = None
            comp_df.rename(columns=cols, inplace=True)
        df = pd.merge(buff, cat, how='outer', on=['Category'])
        df.columns = [col.replace('_x', '').replace("_y", '') for col in df.columns]
        df = df.replace({pd.NA: None}).fillna({'Count': 0})
        self.insert_pois_ranked()
        self.report_worksheet.batch_update([{"range": "E85:K92", "values": df.head(8).values.tolist()}])

    def insert_apartment_ranked(self):
        a = pd.DataFrame(self.data['data']['i15mind']['apartments'])['top_pois']
        from functools import reduce
        df = pd.DataFrame(reduce(lambda a, b: a + b, a.values.tolist())).sort_values(by='price',
                                                                                     ascending=False)
        manager.write_dataframe_to_worksheet(df, self.sheet.get_worksheet(self.worksheets['apartments']))

    def update_apartments(self):
        price_d = dict(
            pd.DataFrame(self.data['data']['i15mind']['apartments'])[['trans_type', 'median_price']].values.tolist())
        # self.insert_pois_ranked()
        self.report_worksheet.batch_update(
            [{"range": "I118", "values": [[price_d.get("rent", 'N_A')]]}, {"range": "J118", "values": [
                [price_d.get("buy")]]}])
        self.insert_apartment_ranked()

    def update_competitor_count(self):
        try:
            price_d1 = dict(pd.DataFrame(self.data['data']['i15mind']['competition']['pois']).groupby(
                'type').id.count().reset_index().values.tolist())
        except KeyError:
            price_d1 = {}
        try:
            price_d2 = dict(pd.DataFrame(self.data['data']['i500mtd']['competition']['pois']).groupby(
                'type').id.count().reset_index().values.tolist())
        except KeyError:
            price_d2 = {}

        # self.insert_pois_ranked()
        self.report_worksheet.batch_update(
            [{"range": "J9", "values": [[price_d2.get("primary_competitor", 0)]]}, {"range": "K9", "values": [
                [price_d1.get("primary_competitor", 0)]]},
             {"range": "J10", "values": [[price_d2.get("anchor_competitor", 0)]]}, {"range": "K10", "values": [
                [price_d1.get("anchor_competitor", 0)]]}])
        self.insert_apartment_ranked()

    def update_high_streets(self):
        df_ = pd.DataFrame(self.data['data']['i15mind']['high_streets'])  # [['cluster_name', 'distance', 'area', ]]
        df = df_.copy()
        cols = {"cluster_name": "High Streets", "Avg Daily visits": "Avg Daily visits",
                "% of Catchment visitors": "% of Catchment visitors", "distance": "Distance from Site",
                'area': "Total Area"}
        for k, v in cols.items():
            if k not in df.columns.tolist():
                df[k] = None
        df.rename(columns=cols, inplace=True)
        df = df[cols.values()]
        df['Total Area'] = df['Total Area'].apply(lambda x: f"{x / 10 ** 6:.2f} sqkm")
        df['Distance from Site'] = df['Distance from Site'].apply(lambda x: f"{x} km")
        manager.write_dataframe_to_worksheet(df_, self.sheet.get_worksheet(self.worksheets['high_streets']))
        self.report_worksheet.batch_update([{"range": "E107:I110", "values": df.head(4).values.tolist()}])

    def update_competition(self):
        comp_df_ = pd.DataFrame(self.data['data']['i15mind']['competition']['pois'])
        if "reviews_per_day" in comp_df_.columns.tolist():
            comp_df_ = comp_df_.sort_values(by='reviews_per_day', ascending=False)
        comp_df = comp_df_.copy()
        cols = {"name": "Brand", "type": "Type", "distance": "Distance from Location",
                "Avg Daily visits": "Avg Daily visits",
                "Performance": "Performance", "number_of_votes": "# of Google Review", "ratings": "Google ratings"}
        for k, v in cols.items():
            if k not in comp_df.columns.tolist():
                comp_df[k] = None
        comp_df = comp_df.rename(columns=cols)
        manager.write_dataframe_to_worksheet(comp_df_, self.sheet.get_worksheet(self.worksheets['competition']))
        comp_df = comp_df[list(cols.values())]
        self.report_worksheet.batch_update([{"range": "E62:K66", "values": comp_df.head(5).values.tolist()}])

    def update_shopping_malls(self):
        comp_df_ = pd.DataFrame(self.data['data']['i15mind']['shopping_malls']['pois'])
        comp_df = comp_df_.copy()
        cols = {"name": "name",
                "Avg Daily visits": "Avg Daily visits", "distance": "Distance from Location",
                "Performance": "Performance", }
        for k, v in cols.items():
            if k not in comp_df.columns.tolist():
                comp_df[k] = None
        comp_df = comp_df.rename(columns=cols)
        manager.write_dataframe_to_worksheet(comp_df_, self.sheet.get_worksheet(self.worksheets['shopping_malls']))
        comp_df = comp_df[list(cols.values())]
        self.report_worksheet.batch_update([{"range": "E99:H103", "values": comp_df.head(5).values.tolist()}])

    def update_projects(self):
        comp_df_ = pd.DataFrame(self.data['data']['i15mind']['projects']['projects']).sort_values(by='final_num_units',
                                                                                                   ascending=False)
        comp_df = comp_df_.copy()
        cols = {"name": "Apartments", "Site Visitor count": "Site Visitor count",
                "% of total visitors": "% of total visitors", "distance": "Distance from site",
                "Medium Rent": "Medium Rent", "Median Sale Price": "Median Sale Price",
                "final_num_units": "Number of Units"}
        for k, v in cols.items():
            if k not in comp_df.columns.tolist():
                comp_df[k] = None
        comp_df = comp_df.rename(columns=cols)
        comp_df = comp_df[list(cols.values())]
        comp_df['Distance from site'] = comp_df['Distance from site'].apply(lambda x: f"{x} km")
        manager.write_dataframe_to_worksheet(comp_df_, self.sheet.get_worksheet(self.worksheets['projects']))
        self.report_worksheet.batch_update([{"range": "E121:K124", "values": comp_df.head(4).values.tolist()}])

    def population_chart(self):
        pop = pd.DataFrame([self.data['data']['i15mind']['population']]).T.reset_index(names=['pop']).query(
            "pop.str.startswith('age_')").values.tolist()
        self.report_worksheet.batch_update([{"range": "N18:O35", "values": pop}])

    @staticmethod
    def get_dictionary_value(d, keys):
        keys = keys.split(".")
        for i in keys:
            d = d.get(i)
        return d

    def generate_report(self):
        # self.generate_report_data()
        self.create_data_worksheets()
        self.clear_google_sheet()
        self.populate_single_cells()
        self.update_projects()
        self.population_chart()
        self.update_competition()
        self.update_category_count()
        self.update_high_streets()
        self.update_shopping_malls()
        self.update_apartments()
        self.update_competitor_count()
        self.update_top_brands()


if __name__ == '__main__':
    env = 'prod'
    folder_ids = {"test": '1g5QIquo6GZNA5wDQxfKI9KkYGNxQ2ukO', "prod": '1ILAdxTEs8wxWkH_7vLRm9mk04CNzd9sH'}
    sheet_cols = {"test": 'google_sheet_id_test', "prod": 'google_sheet_id'}
    projects = ['130110_775547', '130009_776325', '130552_777638', '130631_776205', '128873_775969', '129696_776307',
                '130959_775791']
    projects=['129342_777438', '128050_776996']
    for proj_id in projects[1:]:
        d = Dashboard(proj_id=proj_id)
        d.generate_report()
        time.sleep(30)
