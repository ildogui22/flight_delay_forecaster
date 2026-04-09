.PHONY: localstack ingest verify etl

localstack:
	docker run --rm -p 4566:9000 -p 4567:9001 \
	  -e MINIO_ROOT_USER=minioadmin \
	  -e MINIO_ROOT_PASSWORD=minioadmin \
	  minio/minio server /data --console-address ":9001"


ingest:
	cd src && python -m ingestion.ingest

etl:
	cd src && python -m etl.process


verify:
	AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
	aws --endpoint-url=http://localhost:9000 s3 ls s3://ml-pipeline-raw/ --recursive