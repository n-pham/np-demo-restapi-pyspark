from fastapi import FastAPI, HTTPException, Depends, status
from datetime import date, timedelta
from os import environ
from dotenv import load_dotenv, find_dotenv
import logging
import motor.motor_asyncio
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from apiserver.security import Token, User, get_current_active_user, authenticate_user, create_access_token


load_dotenv(find_dotenv())
DATA_STORE_URI = environ.get("DATA_STORE_URI")
DATA_STORE_DATABASE = environ.get("DATA_STORE_DATABASE")
DATA_STORE_PRODUCT_DAILY_DATA_TABLE = environ.get("DATA_STORE_PRODUCT_DAILY_DATA_TABLE")
SECRET_KEY = environ.get("SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(environ.get("ACCESS_TOKEN_EXPIRE_MINUTES"))
logging.info(f'DATA_STORE_URI: {DATA_STORE_URI}')
logging.info(f'DATA_STORE_DATABASE: {DATA_STORE_DATABASE}')
logging.info(f'DATA_STORE_PRODUCT_DAILY_DATA_TABLE: {DATA_STORE_PRODUCT_DAILY_DATA_TABLE}')
# do not log the SECRET_KEY :)
logging.info(f'ACCESS_TOKEN_EXPIRE_MINUTES: {ACCESS_TOKEN_EXPIRE_MINUTES}')

app = FastAPI()
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/users/me")
async def read_users_me(current_user: User = Depends(get_current_active_user)):
    return current_user

client = motor.motor_asyncio.AsyncIOMotorClient(DATA_STORE_URI)
db = client[DATA_STORE_DATABASE]


@app.get("/productsoldbyday/")
async def get_product_sold_statistics_by_day(product_id: int, date_str: str, token: str = Depends(oauth2_scheme)):
    criteria =  {
                    "_id": {
                        "product_id": product_id,
                        "date_str": date_str
                    }
                }
    if (results := await db[DATA_STORE_PRODUCT_DAILY_DATA_TABLE].find_one(criteria)) is not None:
        return results

    raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"product_id={product_id}, date_str={date_str} not found")

@app.get("/productsoldlast7days/{product_id}")
async def get_product_sold_statistics_last_7_days(product_id: int, token: str = Depends(oauth2_scheme)):
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
