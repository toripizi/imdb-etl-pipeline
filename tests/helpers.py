import datetime
import inspect

import pyspark
from pyspark.sql import DataFrame

DEFAULT_START_DATE = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)


def create_testing_pyspark_session():
    return (
        pyspark.sql.SparkSession.builder.master("local[1]")
        .appName("pytest pyspark context")
        .config(conf=pyspark.SparkConf())
        .getOrCreate()
    )


def df_to_list(df):
    return list(map(lambda x: x.asDict(True), df.collect()))


def assert_df_schema(df: DataFrame, expected_schema: str):
    assert df._jdf.schema().treeString() == (
        "root\n " + inspect.cleandoc("\n" + expected_schema[2:]) + "\n"
    )
