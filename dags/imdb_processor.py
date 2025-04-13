from datetime import datetime

from airflow import DAG
from operators.imdb_operator import ImdbOperator
from operators.imdb_scraper import ImdbScraperOperator

dag = DAG(
    "imdb_processor",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="@weekly",
)

imdb_scrapping_operator = ImdbScraperOperator(dag=dag, task_id="imdb_scrapping_operator")

imdb_operator = ImdbOperator(
    dag=dag,
    task_id="imdb_processor_operator",
)

imdb_scrapping_operator >> imdb_operator
