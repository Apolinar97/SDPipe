import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from pipeline.config.object_store_config import ObjectStoreConfig
from pipeline.logging_config import get_logger


logger = get_logger(__name__)

class ObjectStore:
    def __init__(self, config: ObjectStoreConfig):      
        self.endpoint = config.endpoint
        self.access_key = config.access_key
        self.secret_key = config.secret_key
        self.region = config.region
        self.bucket_name = config.bucket_name
        client_kwargs = {
            "config": Config(signature_version="s3v4"),
            "region_name": self.region or "us-east-1",
        }
        if self.endpoint:
            client_kwargs["endpoint_url"] = self.endpoint
        if self.access_key:
            client_kwargs["aws_access_key_id"] = self.access_key
        if self.secret_key:
            client_kwargs["aws_secret_access_key"] = self.secret_key
        self.client = boto3.client("s3", **client_kwargs)

    def _bucket(self, bucket_name: str | None = None) -> str:
        return bucket_name or self.bucket_name

    def list_objects(self, prefix: str = "", bucket_name: str | None = None):
        try:
            response = self.client.list_objects_v2(Bucket=self._bucket(bucket_name), Prefix=prefix)
            contents = response.get("Contents", [])
            return [obj["Key"] for obj in contents]
        except ClientError as e:
            logger.exception("Failed to list objects: prefix=%s bucket=%s", prefix, self._bucket(bucket_name))
            return []
    
    def download_object(self, key: str, destination: str, bucket_name: str | None = None):
        try:
            self.client.download_file(self._bucket(bucket_name), key, destination)
        except ClientError as e:
            raise RuntimeError(f"Error downloading object '{key}' to '{destination}': {e}") from e

    def upload_file(self, source: str, key: str, bucket_name: str | None = None):
        try:
            self.client.upload_file(source, self._bucket(bucket_name), key)
        except ClientError as e:
            raise RuntimeError(f"Error uploading file '{source}' to '{key}': {e}") from e
    
    def put_object(self, key:str, data:bytes, content_type:str = "application/json", bucket_name:str| None=None):
        try:
            self.client.put_object(
                Bucket = self._bucket(bucket_name),
                Key = key,
                Body = data,
                ContentType = content_type
            )
        except ClientError as e:
            raise RuntimeError(f'Error putting object {key} to bucket {bucket_name} ') from e
    

    def object_exists(self, key: str, bucket_name: str | None = None) -> bool:
        try:
            self.client.head_object(Bucket=self._bucket(bucket_name), Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def get_object_stream(self, key: str, bucket_name: str | None = None):
        """
        Stream an object from S3/MinIO without downloading to disk.
        Returns a streaming body that can be wrapped with io.TextIOWrapper.
        Raises RuntimeError if the object does not exist or cannot be read.
        """
        try:
            bucket = self._bucket(bucket_name)
            response = self.client.get_object(Bucket=bucket, Key=key)
            return response["Body"]
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise RuntimeError(f"Object not found in bucket {bucket!r}: {key!r}") from e
            raise
