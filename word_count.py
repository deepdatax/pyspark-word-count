from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType


def word_count(input: str):
    spark = SparkSession.builder.appName("word_count").getOrCreate()
    schema = StructType([StructField('text', StringType(), True)])
    df: DataFrame = spark.createDataFrame([(input,)], schema=schema)
    df.show()
    """
    1. Do word count
    2. Order by count
    3. Rename count to sum
    """

    word_count.show()


if __name__ == '__main__':
    input = "Hello World, Hello Databricks."

    word_count(input)