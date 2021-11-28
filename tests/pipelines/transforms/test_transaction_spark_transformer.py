from pipelines.transforms.transaction_spark_transformer import TransactionSparkTransformer
from pyspark.sql.types import StructType, StructField, LongType, ArrayType
from collections import Counter

TRANSACTIONS_INPUT_SCHEMA = StructType([
    StructField("id", LongType(), True),
    StructField("products", ArrayType(LongType()), True)])

TRANSACTIONS_OUTPUT_SCHEMA = StructType([
    StructField("product_id", LongType(), True),
    StructField("count", LongType(), False)])

def are_dfs_equal(df1, df2):
    if df1.schema != df2.schema:
        return False
    c1, c2 = Counter(df1.collect()), Counter(df2.collect())
    if c1 != c2:
        return False
    return True

def test_transform_empty(spark_session):
    empty_df = spark_session.createDataFrame(list(),TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    t = TransactionSparkTransformer(spark_session)
    assert t.transform(empty_df).count() == 0

def test_transform_one_row_one_product(spark_session):
    test_data = [
        (100,[999])
    ]
    expected_data = [
        (999,1)
    ]
    test_df = spark_session.createDataFrame(test_data,TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    expected_df = spark_session.createDataFrame(expected_data,TRANSACTIONS_OUTPUT_SCHEMA,verifySchema=False)
    actual_df = TransactionSparkTransformer(spark_session).transform(test_df)
    assert are_dfs_equal(expected_df,actual_df)

def test_transform_one_row_two_product(spark_session):
    test_data = [
        (100,[999,888])
    ]
    expected_data = [
        (999,1),
        (888,1)
    ]
    test_df = spark_session.createDataFrame(test_data,TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    expected_df = spark_session.createDataFrame(expected_data,TRANSACTIONS_OUTPUT_SCHEMA,verifySchema=False)
    actual_df = TransactionSparkTransformer(spark_session).transform(test_df)
    assert are_dfs_equal(expected_df,actual_df)

def test_transform_two_row_one_product(spark_session):
    test_data = [
        (100,[999]),
        (200,[999])
    ]
    expected_data = [
        (999,2)
    ]
    test_df = spark_session.createDataFrame(test_data,TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    expected_df = spark_session.createDataFrame(expected_data,TRANSACTIONS_OUTPUT_SCHEMA,verifySchema=False)
    actual_df = TransactionSparkTransformer(spark_session).transform(test_df)
    assert are_dfs_equal(expected_df,actual_df)

def test_transform_two_row_three_product(spark_session):
    test_data = [
        (100,[999,888]),
        (200,[999,777])
    ]
    expected_data = [
        (999,2),
        (888,1),
        (777,1)
    ]
    test_df = spark_session.createDataFrame(test_data,TRANSACTIONS_INPUT_SCHEMA,verifySchema=False)
    expected_df = spark_session.createDataFrame(expected_data,TRANSACTIONS_OUTPUT_SCHEMA,verifySchema=False)
    actual_df = TransactionSparkTransformer(spark_session).transform(test_df)
    assert are_dfs_equal(expected_df,actual_df)