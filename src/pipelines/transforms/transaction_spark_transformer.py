from dotenv import load_dotenv, find_dotenv
from os import environ
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode, struct, lit
import logging

class TransactionSparkTransformer():
    """
    Class to encapsulate the Transactions transformer logic

    Attributes
    ----------
    spark_session : SparkSession
    SPARK_NUMBER_OF_PARTITION: the number of partitions to use for repartition

    Methods
    -------
    transform(df: DataFrame) -> DataFrame:
        Applies transformation the input Transactions df, does not add any Spark Action.
    """

    def __init__(self, spark_session):
        """
        Constructs the spark session for this Transactions transformer object.

        Parameters
        ----------
            spark_session : SparkSession
        """

        DEFAULT_SPARK_NUMBER_OF_PARTITION = 4

        load_dotenv(find_dotenv())

        self.SPARK_NUMBER_OF_PARTITION = environ.get("SPARK_NUMBER_OF_PARTITION", DEFAULT_SPARK_NUMBER_OF_PARTITION)
        logging.info(f'SPARK_NUMBER_OF_PARTITION: {self.SPARK_NUMBER_OF_PARTITION}')

        self.spark_session = spark_session

    def transform(self, df: DataFrame, transaction_date: str) -> DataFrame:
        """
        Applies transformation the input Transactions df, does not add any Spark Action.

        Parameters
        ----------
        df : Spark DataFrame
            StructType([
                StructField("id", LongType(), True),
                tructField("products", ArrayType(LongType()), True)])
        transaction_date: str (YYYYMMDD)

        Returns
        -------
        Spark DataFrame
            StructType([
                StructField("product_id", LongType(), True),
                StructField("count", LongType(), False)])
        """

        part_df = df.repartition(self.SPARK_NUMBER_OF_PARTITION)
        explode_df = (part_df.select(part_df.id,explode(part_df.products))
                        .withColumnRenamed('col', 'product_id'))
        count_df = explode_df.groupBy('product_id').count()
        # change to this structure {"_id": {product_id,date}, "count": count}
        struct_df = count_df.select(
                                struct(
                                    'product_id',
                                    lit(transaction_date).alias('date_str')
                                ).alias('_id'),
                                'count')

        # DataFrame.explain() does not support INFO log level
        # code will break if this internal implementation is changed
        # logging.info(count_df._sc._jvm.PythonSQLUtils.explainString(count_df._jdf.queryExecution(), 'simple'))

        return struct_df

if __name__ == "__main__":
    pass