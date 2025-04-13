import logging
import os

import pyspark
from airflow.models import Variable

DEFAULT_LIMITS = {}


def get_spark_session(job_name, dag_id, spark_conf={}):
    conf = pyspark.SparkConf()
    dag_config = Variable.get(key=dag_id, default_var={}, deserialize_json=True)
    number_of_cores = dag_config.get(f"{job_name}-cores", 1)
    spark_master_addr = f"local[{number_of_cores}]"
    logging.info(f"Spark master address for {job_name}: {spark_master_addr}")

    conf.setMaster(spark_master_addr)
    conf.setAppName(f"{dag_id}-{job_name}")
    conf.set("spark.jars", f"{os.getenv('SPARK_HOME')}/jars/hadoop-azure-3.3.1.jar")
    conf.set("spark.port.maxRetries", 60)
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    for key, value in spark_conf.items():
        conf.set(key, value)

    dag_config = Variable.get(key=dag_id, default_var={}, deserialize_json=True)
    extra_config = dict(DEFAULT_LIMITS)
    extra_config.update(dag_config.get(job_name, {}))

    logging.info(f"Applying specific spark settings for job {job_name}:\n{extra_config}")

    for key, value in extra_config.items():
        conf.set(key, value)

    sc = pyspark.SparkContext.getOrCreate(conf)

    spark = pyspark.sql.SparkSession(sc)
    logging.info(spark.sparkContext.uiWebUrl)
    return spark
