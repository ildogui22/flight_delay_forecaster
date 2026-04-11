.PHONY: localstack postgres ingest verify bronze-to-silver silver-to-postgres dbt-run dbt-test dbt-debug

localstack:
	docker run --rm -d -p 4566:9000 -p 4567:9001 \
	  -e MINIO_ROOT_USER=minioadmin \
	  -e MINIO_ROOT_PASSWORD=minioadmin \
	  minio/minio server /data --console-address ":9001"

postgres:
	docker run --rm -d \
	  --name ml-postgres \
	  -e POSTGRES_USER=mlpipeline \
	  -e POSTGRES_PASSWORD=mlpipeline \
	  -e POSTGRES_DB=mlpipeline \
	  -p 5432:5432 \
	  postgres:15

ingest:
	cd src && python -m ingestion.ingest

bronze-to-silver:
	cd src && python -m etl.bronze_to_silver

silver-to-postgres:
	cd src && python -m etl.silver_to_postgres

verify:
	AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
	aws --endpoint-url=http://localhost:9000 s3 ls s3://ml-pipeline-raw/ --recursive

dbt-run:
	cd dbt && dbt run --profiles-dir .

dbt-test:
	cd dbt && dbt test --profiles-dir .

dbt-debug:
	cd dbt && dbt debug --profiles-dir .





