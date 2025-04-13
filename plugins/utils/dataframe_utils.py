import itertools
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def convert_to_int(df: DataFrame, column: str) -> DataFrame:
    return df.withColumn(column, F.col(column).cast("int"))


def create_map_from_dict(map_dict: dict):
    return F.create_map([F.lit(x) for x in itertools.chain(*map_dict.items())])


def explode_key(df: DataFrame, key: str) -> DataFrame:
    return df.selectExpr(f"explode({key}) as (id, {key})")


def merge_dfs(result_dfs: list[DataFrame], key: str = "id") -> DataFrame:
    return reduce(lambda df1, df2: df1.join(df2, key), result_dfs)
