import logging
import os

import pyspark.sql.functions as F
from airflow.models import BaseOperator
from pyspark.sql import DataFrame
from schemas.imdb_schema import imdb_schema
from utils.azure_utils import get_azure_path
from utils.dataframe_utils import (
    convert_to_int,
    create_map_from_dict,
    explode_key,
    merge_dfs,
)
from utils.spark import get_spark_session


class ImdbOperator(BaseOperator):
    """
    This operator is used to load the IMDB scrapped data from the Azure Data Lake,
    clean it and transform it into a parquet file.
    """

    def __init__(self, *args, **kwargs):
        super(ImdbOperator, self).__init__(*args, **kwargs)
        self.spark = None
        self.base_path = None

    def setup_spark_session(self, context):
        self.spark = get_spark_session(
            self.task_id,
            dag_id=context["dag"].dag_id,
            spark_conf={
                "spark.sql.adaptive.optimizeSkewedJoin.enabled": "true",
                "fs.azure.account.key.toripizi.dfs.core.windows.net": os.environ.get(
                    "AZURE_ACCOUNT_KEY"
                ),
            },
        )

    def execute(self, context):
        self.setup_spark_session(context)
        self.setup_azure_base_path(context)
        df = self.load_imdb_data()
        df = self.transform_imdb(df)
        df = self.clean_imdb(df)
        self.write_to_parquet(df)

    def setup_azure_base_path(self, context):
        self.base_path = get_azure_path(context["ds"])
        logging.info(f"base path: {self.base_path}")

    def load_imdb_data(self) -> DataFrame:
        logging.info("loading imdb data")
        return (
            self.spark.read.schema(imdb_schema)
            .option("multiline", "true")
            .json(f"{self.base_path}/imdb_processed.json")
        )

    def transform_imdb(self, df: DataFrame) -> DataFrame:
        logging.info("transforming imdb data")
        keys = [
            "cast",
            "composer",
            "country",
            "director",
            "genre",
            "kind",
            "language",
            "rating",
            "runtime",
            "title",
            "vote",
            "writer",
            "year",
        ]

        result_dfs = []
        for key in keys:
            result_dfs.append(explode_key(df, key))

        return merge_dfs(result_dfs)

    def clean_imdb(self, df: DataFrame) -> DataFrame:
        logging.info("cleaning imdb data")
        df = convert_to_int(df, "year")
        df = convert_to_int(df, "vote")
        df = convert_to_int(df, "runtime")

        logging.info(f"dataframe count: {df.count()}")
        df = df.where(F.col("title").isNotNull() & F.col("year").isNotNull())
        df = df.drop_duplicates(["title", "year"])
        logging.info(f"dataframe count after droping duplicates: {df.count()}")

        df = self.map_category(df)
        return df

    def map_category(self, df: DataFrame) -> DataFrame:
        logging.info("list all movies category:")
        df.select("kind").drop_duplicates().show(truncate=0)

        kind_mapping = {
            "movie": "movie",
            "video movie": "movie",
            "tv movie": "movie",
            "tv series": "series",
            "tv mini series": "series",
            "tv short": "series",
            "episode": "series",
            "video game": "game",
        }
        kind_mapper = create_map_from_dict(kind_mapping)
        df = df.withColumn("kind", kind_mapper[F.col("kind")])

        logging.info("list all movies category after mapping:")
        df.select("kind").drop_duplicates().show(truncate=0)
        return df

    def write_to_parquet(self, df: DataFrame):
        logging.info("writing imdb data to parquet")
        df.write.mode("overwrite").parquet(f"{self.base_path}/imdb_processed.parquet")
