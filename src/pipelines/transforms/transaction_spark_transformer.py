from pyspark.sql.dataframe import DataFrame

class TransactionSparkTransformer():

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def transform(self, df):
        assert isinstance(df,DataFrame)
        empty_df = self.spark_session.createDataFrame([],'int',verifySchema=False)
        return empty_df
