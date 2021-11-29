# np-demo-restapi-pyspark

## Business Requirements

Develop an API on which to ask how often a certain product has been sold in the past days.

Every day the department will receive a file with the sales of the previous day. The file is a json dump, each line contains a transaction:

`{"Id": customer id, "products": list of product_ids associated with the transactions}`

(EXTRA) - add authorization

## Architecture and Technology Choices

```
File Storage                Local for demo
|
↓
Batch Processing            PySpark for distributed processing, Airflow to schedule, monitor 
|
↓
Data Store                  key-value, key = (product_id,date) => MongoDB
|
↓
API web server              Auth can re-use this Data Store
```

Ideas
* Later, Airflow FileSensor can be used to trigger Batch Processing by event instead of scheduling
* key = (product_id,date_str) to support query by a product_id and a range of date_str
  MongoDB _id can be composite and also indexed so query will be optimal (see Notes section below)
* Use date_str instead of date because Python datetime.date has no timezone but MongoDB Spark Connector assumes local timezone and convert to UTC so the dates are changed
* MongoDB Spark Connector was chosen to save directly from Spark into MongoDB, and it was working. But setting it up was hard - no direct set up from `pip`, maybe `Maven` can automate this setup.
* API `GET /productsoldbyday/?product_id={product_id}&date_str={date_str=}` to get product {product_id} sold statistics by day {date_str=}, it will query MongoDB for key = (product_id,date_str)
* API `GET /productsoldlast7days/{product_id}` to get product {product_id} sold statistics last 7 days, it will aggregate MongoDB for documents matching key in 7 (product_id,date_str) values and sum the count field



API Framework
|                 | Django REST                 | Flask Restful  | FastAPI (chosen)      |
|-----------------|-----------------------------|----------------|-----------------------|
| REST API        | Y                           | Y              | Y                     |
| Maturity        | Y                           | Y              | Rather New            |
| Auth            | Y                           | Y              | Y                     |
| Use case        | Big framework               | lightweight    | Rapid new development |
| Extras          |                             |                |                       |
| API versioning  | Y                           | Y              | Y                     |
| Built-in Test   | N                           | N              | Y                     |
| Async if needed | In progress                 | Y              | Y                     |
| Features 1      | django-rest-swagger         |                | built-in Swagger      |
| Features 2      | Browsable API (HTML output) | representation |                       |
| Features 3      |                             |                | built-in validation   |
| Features 4      |                             |                | fastapi-versioning    |

## Assumptions

* File name format is `YYYYMMDD_transactions.json`. The date can only be collected from this file name.

## Installing Libraries

### virtualenv
```
cd <full path to your project folder>
python3 -m pip install virtualenv
virtualenv .
```
### Others
```
cd <full path to your project folder>
source bin/activate
pip install -r requirements.txt

```

## Set up Airflow
```
cd <full path to your project folder>
source bin/activate
export AIRFLOW_HOME=./src/airflow
airflow --help
airflow db init
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
airflow webserver -p 8080
# Open http://localhost:8080/ in web browser and log in with admin/admin
```

## Set up MongoDB server
```
MongoDB will be set up outside of your project folder.
Instructions below are for MacOS:
* brew tap mongodb/brew
* brew install mongodb-community
* mongod --config /usr/local/etc/mongod.conf (add `&> /dev/null &` to run in background)
* mongosh
* show dbs
* use <db name>
* db.<table name>.findOne()
* ... <consult MongoDB official documents for more commands>
```

## Set up MongoDB drivers in project
```
**Manual work** for this setup, because I have not found an automatic way to install:
* Download MongoDB Spark Connector from <https://spark-packages.org/package/mongodb/mongo-spark> (e.g. mongo-spark-connector_2.12-3.0.1.jar)
* Put the downloaded jar in <project folder>/lib/pythonx.y/site-packages/pyspark/jars
* In your project folder, run `pyspark --jars lib/pythonx.y/site-packages/pyspark/jars/mongo-spark-connector_2.12-3.0.1.jar`
* Spark will automatically detect and download some more driver jars but in my case they are put in `~/.ivy2/jars` and the command fails
* Copy the jar files in `~/.ivy2/jars` to project folder/lib/pythonx.y/site-packages/pyspark/jars
* In your project folder, run again `pyspark --jars mongo-spark-connector_2.12-3.0.1.jar`, it will succeed this time
```

## How to run
```
cd <full path to your project folder>
# edit .env file
source bin/activate
python setup.py develop
airflow scheduler &> /dev/null &
airflow webserver -p 8080 &> /dev/null &
# Open http://localhost:8080/ in web browser and log in with admin/admin
# start mongodb (`mongod --config /usr/local/etc/mongod.conf` for local)
uvicorn apiserver.main:app --reload
# Open http://localhost:8000/docs in web browser to run the APIs
# The username and password are currently hard-coded as `johndoe` and `secret`
# To have test data, either:
  * Manually trigger Airflow dag `daily_transactions_dag`
  * Wait for Airflow dag `daily_transactions_dag` to automatically based on .env DAG_SCHEDULE_INTERVAL
  * Manually input data into MongoDB using `mongosh` such as `db.products.insertOne( {'_id': {'product_id': 29, 'date_str': '20211127'}, 'count': 1127 } )`
```

## Run pytest
```
cd <full path to your project folder>
source bin/activate
pytest tests
```

## Notes

Performance test for MongoDB composite _id
```
db.testid.insertOne( {_id:{product_id:777,date:new Date("2021-11-27")},count:11})

db.testid.explain().find(
{
    "_id" : {
        "product_id": 777,
        "date" : Date("2021-11-27")
    }
}
)

queryPlanner.winningPlan => `{ stage: 'IDHACK' }`
```

To debug process_daily_transactions function
```
cd <full path to your project folder>
source bin/activate
python -c 'from pipelines.daily_transactions import process_daily_transactions; process_daily_transactions("<file folder>","20190207")'
```

## References
* <https://www.mongodb.com/developer/quickstart/python-quickstart-fastapi/>