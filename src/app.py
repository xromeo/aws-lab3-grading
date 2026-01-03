import json
import os
from io import BytesIO
from typing import Any, Dict
from urllib.parse import unquote_plus

import boto3
from PIL import Image, ExifTags
from dotenv import load_dotenv

# Load .env file if it exists (for local development)
load_dotenv()

# Global clients initialized after load_dotenv
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

def is_supported_image(key: str) -> bool:
    """Check if the file extension is in the allowed list."""
    return key.lower().endswith((".jpg", ".jpeg", ".png"))


def build_metadata_path(input_key: str, input_prefix: str, output_prefix: str) -> str:
    """Construct the output S3 key for metadata files."""
    relative_path = input_key[len(input_prefix):] if input_key.startswith(input_prefix) else input_key
    return f"{output_prefix}{relative_path}.json"

def extract_exif_data(img: Image.Image) -> Dict[str, Any]:
    """Extract EXIF tags and sanitize them for JSON compatibility."""
    exif_map = {}
    try:
        raw_exif = img.getexif()
        if not raw_exif:
            return exif_map

        for tag_id, value in raw_exif.items():
            tag_name = ExifTags.TAGS.get(tag_id, str(tag_id))
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("utf-8", errors="replace")
            elif isinstance(value, (tuple, list)):
                value = [str(v) for v in value]
            else:
                value = str(value)
            exif_map[tag_name] = value
    except Exception as e:
        print(f"Warning: Failed to parse EXIF: {e}")
    return exif_map

def ingest_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for S3 events. Validates uploads and notifies SQS."""
    queue_url = os.getenv("QUEUE_URL")
    input_prefix = os.getenv("INPUT_PREFIX", "incoming/")

    if not queue_url:
        raise RuntimeError("QUEUE_URL is not configured")

    records = event.get("Records", [])
    enqueued_count = 0

    for record in records:
        try:
            bucket = record["s3"]["bucket"]["name"]
            key = unquote_plus(record["s3"]["object"]["key"])
            etag = record["s3"]["object"].get("eTag")

            if not key.startswith(input_prefix) or not is_supported_image(key):
                print(f"Skipping {key}: invalid prefix or extension")
                continue

            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps({"bucket": bucket, "key": key, "etag": etag})
            )
            enqueued_count += 1
        except Exception as e:
            print(f"Error processing S3 record: {e}")

    return {"statusCode": 200, "body": {"enqueued": enqueued_count}}

def metadata_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Entry point for SQS messages. Extracts image info and saves to S3."""
    input_prefix = os.getenv("INPUT_PREFIX", "incoming/")
    output_prefix = os.getenv("OUTPUT_PREFIX", "metadata/")

    processed_count = 0

    for record in event.get("Records", []):
        try:
            message_body = json.loads(record["body"])
            bucket, key = message_body["bucket"], message_body["key"]
            etag = message_body.get("etag")

            target_key = build_metadata_path(key, input_prefix, output_prefix)

            # Idempotency check
            try:
                s3.head_object(Bucket=bucket, Key=target_key)
                print(f"Skip: {target_key} exists.")
                continue
            except s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] not in ("404", "403"):
                    raise e

            # Image processing
            response = s3.get_object(Bucket=bucket, Key=key)
            with response['Body'] as stream:
                raw_data = stream.read()
                with Image.open(BytesIO(raw_data)) as img:
                    metadata = {
                        "source_bucket": bucket,
                        "source_key": key,
                        "width": img.width,
                        "height": img.height,
                        "file_size_bytes": response.get("ContentLength", len(raw_data)),
                        "format": (img.format or "UNKNOWN").upper(),
                    }


            s3.put_object(
                Bucket=bucket,
                Key=target_key,
                Body=json.dumps(metadata, indent=2),
                ContentType="application/json"
            )
            processed_count += 1

        except Exception as e:
            print(f"Fatal error: {e}")
            raise e

    return {"statusCode": 200, "body": {"processed": processed_count}}