
import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


def main():
    endpoint = os.getenv("S3_ENDPOINT", "http://rustfs:9000")
    bucket = os.getenv("S3_BUCKET", "cinema")

    catalogue_path = Path(
        os.getenv(
            "CATALOGUE_PATH",
            "/opt/airflow/data/catalogue.jsonl",
        )
    )
    object_key = "catalogue/catalogue.jsonl"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )

    if not catalogue_path.is_file():
        raise FileNotFoundError(
            f"Catalogue file not found: {catalogue_path}"
        )

    try:
        s3.head_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' already exists.")

    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get(
            "HTTPStatusCode"
        )
        code = error.response.get("Error", {}).get("Code")

        if status == 404 or code in ("NoSuchBucket", "NotFound"):
            s3.create_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' created.")
        else:
            raise

    s3.upload_file(
        str(catalogue_path),
        bucket,
        object_key,
        ExtraArgs={"ContentType": "application/x-ndjson"},
    )

    result = s3.head_object(Bucket=bucket, Key=object_key)

    print(
        f"Catalogue uploaded: s3://{bucket}/{object_key}, "
        f"size={result['ContentLength']} bytes"
    )
    print("S3 initialization completed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"S3 initialization failed: {error}", file=sys.stderr)
        sys.exit(1)
