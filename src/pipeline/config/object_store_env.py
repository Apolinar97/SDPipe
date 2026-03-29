import os

from pipeline.config.object_store_config import ObjectStoreConfig


def require_env(var_name):
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Environment variable '{var_name}' is required but not set.")
    return value


def get_object_store_config(bucket_name_env: str) -> ObjectStoreConfig:
    s3_end_point = os.getenv("AWS_S3_ENDPOINT") or None
    s3_access_key = os.getenv("AWS_S3_ACCESS_KEY") or None
    s3_secret_key = os.getenv("AWS_S3_SECRET_KEY") or None
    s3_region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or None
    s3_bucket_name = require_env(bucket_name_env)

    return ObjectStoreConfig(
        bucket_name=s3_bucket_name,
        endpoint=s3_end_point,
        access_key=s3_access_key,
        secret_key=s3_secret_key,
        region=s3_region,
    )
