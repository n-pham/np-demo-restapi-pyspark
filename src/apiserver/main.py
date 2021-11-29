from fastapi import FastAPI, HTTPException
from datetime import date, timedelta
from os import path, environ
from dotenv import load_dotenv, find_dotenv
import logging
import motor.motor_asyncio
from asgiref.sync import sync_to_async

load_dotenv(find_dotenv())
DATA_STORE_URI = environ.get("DATA_STORE_URI")
DATA_STORE_DATABASE = environ.get("DATA_STORE_DATABASE")
DATA_STORE_PRODUCT_DAILY_DATA_TABLE = environ.get("DATA_STORE_PRODUCT_DAILY_DATA_TABLE")
logging.info(f'DATA_STORE_URI: {DATA_STORE_URI}')
logging.info(f'DATA_STORE_DATABASE: {DATA_STORE_DATABASE}')
logging.info(f'DATA_STORE_PRODUCT_DAILY_DATA_TABLE: {DATA_STORE_PRODUCT_DAILY_DATA_TABLE}')


app = FastAPI()
client = motor.motor_asyncio.AsyncIOMotorClient(DATA_STORE_URI)
db = client[DATA_STORE_DATABASE]


@app.get("/productsoldbyday/")
async def get_product_sold_statistics_by_day(product_id: int, date_str: str):
    criteria =  {
                    "_id": {
                        "product_id": product_id,
                        "date_str": date_str
                    }
                }
    if (results := await db[DATA_STORE_PRODUCT_DAILY_DATA_TABLE].find_one(criteria)) is not None:
        return results

    raise HTTPException(
            status_code=404,
            detail=f"product_id={product_id}, date_str={date_str} not found")

@app.get("/productsoldlast7days/{product_id}")
async def get_product_sold_statistics_last_7_days(product_id: int):
    date_list = [date.today() - timedelta(days=x) for x in range(7)]
    id_list =   [{
                    "product_id": product_id,
                    "date_str": d.strftime('%Y%m%d')
                } for d in date_list]

    criteria =  {
                    "_id": {"$in":id_list}
                }

    resutls = []

    # https://motor.readthedocs.io/en/stable/api-asyncio/asyncio_motor_collection.html#motor.motor_asyncio.AsyncIOMotorCollection.aggregate
    results = await db[DATA_STORE_PRODUCT_DAILY_DATA_TABLE].aggregate([
                        {"$match": criteria},
                        {"$group": { "_id": "$_id.product_id", "count": { "$sum": "$count" } } } ]
                    ).to_list(length=None)

    return results
