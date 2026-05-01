# src/s3_storage.py
"""
AWS S3 storage module.
Uploads raw JSON payloads and transformed CSVs to S3 as a data lake layer.
"""

import json
import logging
import os
from datetime import datetime
from io import StringIO

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')


def get_s3_client():
    """Create and return a boto3 S3 client using env credentials."""
    return boto3.client(
        's3',
        region_name=AWS_REGION,
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    )


def upload_to_s3(data, s3_key, bucket=None):
    """
    Upload a Python dict as JSON to S3.

    Parameters:
        data (dict): Data to serialize and upload
        s3_key (str): S3 object key (path within bucket)
        bucket (str): S3 bucket name (defaults to env var)

    Returns:
        bool: True if successful
    """
    bucket = bucket or S3_BUCKET_NAME
    if not bucket:
        logger.error("❌ S3_BUCKET_NAME not set")
        return False

    try:
        client = get_s3_client()
        body = json.dumps(data, indent=2, default=str)
        client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=body,
            ContentType='application/json',
        )
        logger.info(f"✅ Uploaded to s3://{bucket}/{s3_key}")
        return True

    except NoCredentialsError:
        logger.error("❌ AWS credentials not found — check your .env file")
        return False
    except ClientError as e:
        logger.error(f"❌ S3 upload failed: {e.response['Error']['Message']}")
        return False


def upload_dataframe_to_s3(df, s3_key, bucket=None):
    """
    Upload a Pandas DataFrame as a CSV to S3.

    Parameters:
        df (pd.DataFrame): DataFrame to upload
        s3_key (str): S3 object key
        bucket (str): S3 bucket name

    Returns:
        bool: True if successful
    """
    bucket = bucket or S3_BUCKET_NAME
    if not bucket:
        logger.error("❌ S3_BUCKET_NAME not set")
        return False

    try:
        client = get_s3_client()
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv',
        )
        logger.info(f"✅ Uploaded DataFrame CSV to s3://{bucket}/{s3_key}")
        return True

    except NoCredentialsError:
        logger.error("❌ AWS credentials not found")
        return False
    except ClientError as e:
        logger.error(f"❌ S3 CSV upload failed: {e.response['Error']['Message']}")
        return False


def list_s3_objects(prefix='', bucket=None):
    """
    List objects in S3 bucket under a given prefix.

    Parameters:
        prefix (str): Key prefix to filter by (e.g. 'raw/2025-01-01/')
        bucket (str): S3 bucket name

    Returns:
        list: List of S3 object keys
    """
    bucket = bucket or S3_BUCKET_NAME
    try:
        client = get_s3_client()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = [obj['Key'] for obj in response.get('Contents', [])]
        logger.info(f"✅ Found {len(keys)} objects under s3://{bucket}/{prefix}")
        return keys
    except ClientError as e:
        logger.error(f"❌ Failed to list S3 objects: {e}")
        return []


if __name__ == '__main__':
    # Smoke test — upload a sample record
    sample = {'city': 'Toronto', 'temp': 7.2, 'test': True}
    today = datetime.utcnow().strftime('%Y-%m-%d')
    upload_to_s3(sample, f'raw/{today}/test_toronto.json')
