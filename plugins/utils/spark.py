import logging
import os

import pyspark
from airflow.models import Variable

DEFAULT_CONFIG = {
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.port.maxRetries": 60,
    "spark.jars": f"{os.getenv('SPARK_HOME')}/jars/hadoop-azure-3.3.1.jar",
}


def get_spark_session(job_name: str, dag_id: str, spark_conf: dict = {}):
    conf = pyspark.SparkConf()
    dag_config = Variable.get(key=dag_id, default_var={}, deserialize_json=True)
    number_of_cores = dag_config.get(f"{job_name}-cores", 1)
    additional_config = dag_config.get(job_name, {})

    spark_master_addr = f"local[{number_of_cores}]"
    logging.info(f"Spark master address for {job_name}: {spark_master_addr}")

    conf.setMaster(spark_master_addr)
    conf.setAppName(f"{dag_id}-{job_name}")

    extra_config = dict(DEFAULT_CONFIG)
    extra_config.update(spark_conf)
    extra_config.update(additional_config)

    logging.info(
        f"Applying specific spark settings for job {job_name}:\n{additional_config}"
    )

    for key, value in extra_config.items():
        conf.set(key, value)

    sc = pyspark.SparkContext.getOrCreate(conf)
    spark = pyspark.sql.SparkSession(sc)
    logging.info(spark.sparkContext.uiWebUrl)
    return spark
