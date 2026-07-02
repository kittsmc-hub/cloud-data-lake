.PHONY: up down logs test test-unit fmt lint init dashboard trigger clean

up:
	docker compose up -d --build
	@echo "Airflow:   http://localhost:8080  (admin/admin)"
	@echo "MinIO UI:  http://localhost:9001  (minioadmin/minioadmin123)"
	@echo "Spark UI:  http://localhost:8082"
	@echo "Dashboard: http://localhost:8501"

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

init:
	./infra/minio-init.sh

trigger:
	docker compose exec airflow-scheduler airflow dags trigger log_pipeline_dag

test-unit:
	cd spark-jobs && pip install -r requirements.txt -q && pytest tests/ -v

dbt-run:
	cd dbt/log_analytics && dbt run --profiles-dir .

dbt-test:
	cd dbt/log_analytics && dbt test --profiles-dir .

dashboard:
	cd dashboard && streamlit run app.py

clean:
	docker compose down -v
	find . -name "__pycache__" -type d -exec rm -rf {} +
	find . -name "*.duckdb" -delete
