import logging
import os

from airflow.models import BaseOperator
from pyspark.sql import DataFrame
from schemas.imdb_schema import imdb_schema
from utils.azure_utils import get_azure_path
from utils.spark import get_spark_session


class ImdbScraperOperator(BaseOperator):
    """
    This operator is used to scrape IMDB data from the web and save it to azure data lake.
    """

    def __init__(self, *args, **kwargs):
        super(ImdbScraperOperator, self).__init__(*args, **kwargs)
        self.spark = None

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
        df = self.get_scraped_data()
        self.write_source_data(df, context)

    def get_scraped_data(self) -> DataFrame:
        """
        This method should implement IMDB data scraping functionality.
        However, for the purpose of this assignment, I'll skip the actual scraping
        and load pre-prepared data from a local path instead.

        Returns:
            DataFrame: A PySpark DataFrame containing IMDB data
        """
        logging.info("reading imdb source data from local file")
        return self.spark.read.schema(imdb_schema).json(
            "file:///opt/airflow/plugins/data/imdb_processed.json"
        )

    def write_source_data(self, df: DataFrame, context):
        logging.info("writing imdb source data to azure")
        base_path = get_azure_path(context["ds"])
        df.write.mode("overwrite").json(f"{base_path}/imdb_processed.json")
