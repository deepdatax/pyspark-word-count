from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def word_count(input: str):
    spark = SparkSession.builder.appName("word_count").getOrCreate()
    schema = StructType([StructField('index', IntegerType(), True),
                         StructField('text', StringType(), True)
                         ])
    df: DataFrame = spark.createDataFrame([(1, input)], schema=schema)
    df.show()
    """
    1. Do word count
    2. Order by count
    3. Rename count to sum
    """


if __name__ == '__main__':
    input = "Hello World, Hello Databricks."

    word_count(input)
