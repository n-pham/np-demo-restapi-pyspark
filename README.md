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

## Installing Libraries

### virtualenv
```
cd <full path to your project folder>
python3 -m pip install virtualenv
virtualenv .
```
