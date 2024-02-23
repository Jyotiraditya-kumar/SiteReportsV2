from fastapi import FastAPI, HTTPException
import pandas as pd
import shapely
import calculations_v5 as C
import ast
import utils as U
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import ast

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

templates = Jinja2Templates(directory="templates")


@app.get("/site_report/initial_data")
async def initial_data(
    request: Request,
    lat: float,
    lng: float,
    name: str,
    travel_mode,
    cost_type,
    cost: int,
    primary_comp,
    anchor_comp,
):
    competitor_brand_ids = ast.literal_eval(primary_comp)
    anchor_brand_ids = ast.literal_eval(anchor_comp)
    try:
        resp = U.get_initial_data(
            lat=lat,
            lng=lng,
            name=name,
            travel_mode=travel_mode,
            cost_type=cost_type,
            cost=cost,
            competitor_brand_ids=competitor_brand_ids,
            anchor_brand_ids=anchor_brand_ids,
        )
        return resp
    except Exception as e:
        return HTTPException(status_code=400,detail=str(e))


@app.get("/site_report/secondary_data")
async def initial_data(
    request: Request,
    lat: float,
    lng: float,
    name: str,
    travel_mode,
    cost_type,
    cost: int,
    primary_comp,
    anchor_comp,
    weights,
):
    competitor_brand_ids = ast.literal_eval(primary_comp)
    anchor_brand_ids = ast.literal_eval(anchor_comp)
    weights = ast.literal_eval(weights)
    try:
        resp = U.get_secondary_data(
            lat=lat,
            lng=lng,
            name=name,
            travel_mode=travel_mode,
            cost_type=cost_type,
            cost=cost,
            competitor_brand_ids=competitor_brand_ids,
            anchor_brand_ids=anchor_brand_ids,
            location_score_weights=weights,
        )
        return resp
    except Exception as e:
        return HTTPException(status_code=400,detail=str(e))
