import logging
from os import path, environ
from pyspark.sql import SparkSession
from dotenv import load_dotenv, find_dotenv
from pipelines.transforms.transaction_spark_transformer import TransactionSparkTransformer

def process_daily_transactions(file_location: str, date_part: str) -> None:
    """
    Reads the file specified by file_location and date_part into DataFrame
    Calls TransactionSparkTransformer to transform the DataFrame
    Writes the DataFrame into Data Store

    Parameters
    ----------
    file_location : str
        The file path
    date_part : str
        The date part in the file name

    Returns
    -------
    None
    """

    logging.info(f'file_location: {file_location}')
    logging.info(f'date_part: {date_part}')

    base_filename = f'{date_part}_transactions'
    suffix = '.json'
    file_full_path = path.join(file_location, base_filename + suffix)
    logging.info(f'file_full_path: {file_full_path}')


    DEFAULT_SPARK_NUMBER_OF_PARTITION = 4

    load_dotenv(find_dotenv())

    SPARK_NUMBER_OF_PARTITION = environ.get("SPARK_NUMBER_OF_PARTITION", DEFAULT_SPARK_NUMBER_OF_PARTITION)
    DATA_STORE_URI = environ.get("DATA_STORE_URI")
    DATA_STORE_PRODUCT_DAILY_DATA_TABLE = environ.get("DATA_STORE_PRODUCT_DAILY_DATA_TABLE")
    logging.info(f'SPARK_NUMBER_OF_PARTITION: {SPARK_NUMBER_OF_PARTITION}')
    logging.info(f'DATA_STORE_URI: {DATA_STORE_URI}')
    logging.info(f'DATA_STORE_PRODUCT_DAILY_DATA_TABLE: {DATA_STORE_PRODUCT_DAILY_DATA_TABLE}')

    # get spark session
    spark = (SparkSession
        .builder
        .master(f"local[{SPARK_NUMBER_OF_PARTITION}]")
        .appName("np-demo-restapi-pyspark")
        .config("spark.default.parallelism",SPARK_NUMBER_OF_PARTITION)
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") 
        .config("spark.mongodb.output.uri", DATA_STORE_URI)
        .getOrCreate())

    # read file
    df = (spark.read.json(file_full_path))

    # apply transformation
    count_df = TransactionSparkTransformer(spark).transform(df,date_part)
    logging.info(count_df.columns)

    # write into data store
    count_df.write.format("mongo").mode("append") \
                    .option("collection", DATA_STORE_PRODUCT_DAILY_DATA_TABLE) \
                    .save()


if __name__ == "__main__":
    pass