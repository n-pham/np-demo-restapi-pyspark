from dotenv import load_dotenv, find_dotenv
from os import environ
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode
import logging

load_dotenv(find_dotenv())

SPARK_NUMBER_OF_PARTITION = environ.get("SPARK_NUMBER_OF_PARTITION", 4)
logging.info(f'SPARK_NUMBER_OF_PARTITION: {SPARK_NUMBER_OF_PARTITION}')

class TransactionSparkTransformer():

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def transform(self, df):
        assert isinstance(df,DataFrame)

        part_df = df.repartition(SPARK_NUMBER_OF_PARTITION)
        explode_df = (part_df.select(part_df.id,explode(part_df.products))
                        .withColumnRenamed('col', 'product_id'))
        count_df = explode_df.groupBy('product_id').count()

        # logging.info(count_df._sc._jvm.PythonSQLUtils.explainString(count_df._jdf.queryExecution(), 'simple'))

        return count_df
