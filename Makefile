TAG?=dev

build:
	docker build -t airflow:$(TAG) .

shell:
	docker compose run -e PYTHONPATH="/opt/airflow/plugins" airflow-scheduler bash

start:
	docker compose up --detach;
	@echo "\nvisit http://localhost:8001/ and login as airflow:airflow\n"

stop:
	docker compose down
