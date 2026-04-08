.PHONY: localstack ingest verify

localstack:
	docker run --rm -p 9000:9000 \
	  -e MINIO_ROOT_USER=minioadmin \
	  -e MINIO_ROOT_PASSWORD=minioadmin \
	  minio/minio server /data

ingest:
	cd src && python -m ingestion.ingest

verify:
	AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
	aws --endpoint-url=http://localhost:9000 s3 ls s3://ml-pipeline-raw/ --recursive