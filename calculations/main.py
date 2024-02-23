from fastapi import FastAPI, Query
import pandas as pd
import shapely
import calculations_v5 as C
import utils as U
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import ast

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


@app.get("/site_report/create")
async def root(request: Request, lat: float, lng: float, name: str, travel_mode, cost_type, cost: int):
    res = U.create_project(lat=lat, lng=lng, name=name, travel_mode=travel_mode, cost_type=cost_type, cost=cost, )
    return res


# 
# 
# @app.get("/items/{id}", response_class=HTMLResponse)
# async def read_item(request: Request, id: str):
#     return templates.TemplateResponse(
#         request=request, name="item.html", context={"id": id}
#     )

@app.get("/site_report/{report_id}/{id}/cft")
async def cft(report_id, id):
    avg_cft = C.get_avg_cft(report_id=report_id, id=id)
    return avg_cft


@app.get("/site_report/{report_id}/{id}/population")
async def population(report_id, id):
    resp = C.get_population_index(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/companies")
async def companies(report_id, id):
    resp = C.get_companies(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/demand_generator")
async def demand_generators(report_id, id):
    resp = C.get_demand_generator_data(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/projects")
async def projects(report_id, id):
    resp = C.get_projects(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/affluence")
async def affluence(report_id, id):
    resp = C.get_affluence_index(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/income")
async def income(report_id, id):
    resp = C.get_income_distribution(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/competition")
async def competition(report_id, id):
    resp = C.get_competition(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/shopping_mall")
async def competition(report_id, id):
    resp = C.get_malls(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/top_brands")
async def top_brands(report_id, id):
    resp = C.get_top_brands(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/category_count")
async def category_count(report_id, id):
    resp = C.get_category_count(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/property_price")
async def property_price(report_id, id):
    resp = C.get_rent_and_sale_prices(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/high_street")
async def high_street(report_id, id):
    resp = C.get_high_streets(report_id=report_id, id=id)
    return resp


@app.get("/site_report/{report_id}/{id}/initial_data")
async def initial_data(report_id, id):
    resp = dict(avg_cost_for_two=C.get_avg_cft(report_id=report_id, id=id),
                population=C.get_population_index(report_id=report_id, id=id),
                demand_generators=C.get_demand_generator_data(report_id=report_id, id=id),
                companies=C.get_companies(report_id=report_id, id=id),
                household_distribution=C.get_income_distribution(report_id=report_id, id=id),
                # apartments=C.get_rent_and_sale_prices(report_id=report_id, id=id)
                )
    return resp


@app.get("/site_report/{report_id}/{id}/secondary_data")
async def secondary_data(report_id, id):
    resp = dict(projects=C.get_projects(report_id=report_id, id=id),
                apartments=C.get_rent_and_sale_prices(report_id=report_id, id=id),
                shopping_malls=C.get_malls(report_id=report_id, id=id),
                demand_generators=C.get_demand_generator_data(report_id=report_id, id=id),
                competition=C.get_competition(report_id=report_id, id=id),
                # household_distribution=C.get_income_distribution(report_id=report_id, id=id),

                )
    return resp
