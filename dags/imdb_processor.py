from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from operators.imdb_operator import ImdbOperator


dag = DAG(
    "imdb_processor",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="@weekly",
)

# here should be implemented operator which scrap data from IMDB
empty_scrapping_operator = EmptyOperator(task_id="empty_scrapping_operator")

imdb_operator = ImdbOperator(
    dag=dag,
    task_id="imdb_processor_operator",
)

empty_scrapping_operator >> imdb_operator
