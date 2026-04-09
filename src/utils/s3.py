import json
import os

import boto3
from dotenv import load_dotenv

load_dotenv()

def get_client():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "eu-west-1"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

def upload_json(data: dict | list, bucket: str, key: str):
    client = get_client()
    client.put_object(
        Bucket = bucket,
        Key = key,
        Body = json.dumps(data),
        ContentType = "application/json"
    )

def download_json(bucket: str, key: str) -> dict | list:
    client = get_client()
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read())

def ensure_bucket(bucket: str) -> None:
    client = get_client()
    try:
        client.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": os.getenv("AWS_REGION", "eu-west-1")}
        )
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass