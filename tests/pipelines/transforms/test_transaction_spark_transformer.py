from pipelines.transforms.transaction_spark_transformer import TransactionSparkTransformer
from pyspark.sql.types import StructType, StructField, LongType, ArrayType, StringType
from collections import Counter
from pyspark.sql import Row
import logging

TRANSACTION_DATE = '20211127'

TRANSACTIONS_INPUT_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("products", ArrayType(LongType()), True)])

EXPECTED_TRANSACTIONS_OUTPUT_SCHEMA = StructType([
    StructField("_id",StructType([
        StructField("product_id", LongType(), True),
        StructField("date_str", StringType(), False)
        ]),False),
    StructField("count", LongType(), False)])

def are_dfs_equal(df1, df2):
    """
    Returns Boolean.

    Unit Test utility function to check if 2 Spark DataFrames are equal

    Parameters
    ----------
    df1 : Spark DataFrame
    df2 : Spark DataFrame

    Returns
    -------
    Boolean
    """

    if df1.schema != df2.schema:
        logging.info(f'First: {df1.schema}')
        logging.info(f'Second: {df2.schema}')
        return False
    # The 2 lists returned by collect() can have different element ordering,
    # so Counter is needed to compare regardless of element ordering
    c1, c2 = Counter(df1.collect()), Counter(df2.collect())
    if c1 != c2:
        return False
    return True

def test_transform_empty(spark_session):
    empty_df = spark_session.createDataFrame(list(),TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    t = TransactionSparkTransformer(spark_session)
    assert t.transform(empty_df,TRANSACTION_DATE).count() == 0

def test_transform_one_row_one_product(spark_session):
    test_data = [
        (100,[999])
    ]
    expected_data = [
        Row(_id=Row(product_id=999, date_str=TRANSACTION_DATE),count=1)
    ]
    test_df = spark_session.createDataFrame(test_data,TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    expected_df = spark_session.createDataFrame(expected_data,EXPECTED_TRANSACTIONS_OUTPUT_SCHEMA,verifySchema=False)
    actual_df = TransactionSparkTransformer(spark_session).transform(test_df,TRANSACTION_DATE)
    assert are_dfs_equal(expected_df,actual_df)

def test_transform_one_row_two_product(spark_session):
    test_data = [
        (100,[999,888])
    ]
    expected_data = [
        Row(_id=Row(product_id=999, date_str=TRANSACTION_DATE),count=1),
        Row(_id=Row(product_id=888, date_str=TRANSACTION_DATE),count=1)
    ]
    test_df = spark_session.createDataFrame(test_data,TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    expected_df = spark_session.createDataFrame(expected_data,EXPECTED_TRANSACTIONS_OUTPUT_SCHEMA,verifySchema=False)
    actual_df = TransactionSparkTransformer(spark_session).transform(test_df,TRANSACTION_DATE)
    assert are_dfs_equal(expected_df,actual_df)

def test_transform_two_row_one_product(spark_session):
    test_data = [
        (100,[999]),
        (200,[999])
    ]
    expected_data = [
        Row(_id=Row(product_id=999, date_str=TRANSACTION_DATE),count=2)
    ]
    test_df = spark_session.createDataFrame(test_data,TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    expected_df = spark_session.createDataFrame(expected_data,EXPECTED_TRANSACTIONS_OUTPUT_SCHEMA,verifySchema=False)
    actual_df = TransactionSparkTransformer(spark_session).transform(test_df,TRANSACTION_DATE)
    assert are_dfs_equal(expected_df,actual_df)

def test_transform_two_row_three_product(spark_session):
    test_data = [
        (100,[999,888]),
        (200,[999,777])
    ]
    expected_data = [
        Row(_id=Row(product_id=999, date_str=TRANSACTION_DATE),count=2),
        Row(_id=Row(product_id=888, date_str=TRANSACTION_DATE),count=1),
        Row(_id=Row(product_id=777, date_str=TRANSACTION_DATE),count=1)
    ]
    test_df = spark_session.createDataFrame(test_data,TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    expected_df = spark_session.createDataFrame(expected_data,EXPECTED_TRANSACTIONS_OUTPUT_SCHEMA,verifySchema=False)
    actual_df = TransactionSparkTransformer(spark_session).transform(test_df,TRANSACTION_DATE)
    assert are_dfs_equal(expected_df,actual_df)