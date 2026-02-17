import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

class ObjectStore:
    def __init__(self):
        self.endpoint = os.getenv("OBJECT_STORE_ENDPOINT")
        self.access_key = os.getenv("OBJECT_STORE_ACCESS_KEY")
        self.secret_key = os.getenv("OBJECT_STORE_SECRET_KEY")
        self.bucket_name = os.getenv("OBJECT_STORE_BUCKET_NAME", "sdpipe")
        client_kwargs = {"config": Config(signature_version="s3v4"), "region_name": "us-east-1"}
        if self.endpoint:
            client_kwargs["endpoint_url"] = self.endpoint
        if self.access_key:
            client_kwargs["aws_access_key_id"] = self.access_key
        if self.secret_key:
            client_kwargs["aws_secret_access_key"] = self.secret_key
        self.client = boto3.client("s3", **client_kwargs)

    def list_objects(self, prefix: str = ""):
        try:
            response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            contents = response.get("Contents", [])
            return [obj["Key"] for obj in contents]
        except Exception as e:
            print(f"Error listing objects with prefix '{prefix}': {e}")
            return []
    
    def download_object(self, key: str, destination: str):
        try:
            self.client.download_file(self.bucket_name, key, destination)
            print(f"Downloaded object '{key}' to '{destination}'")
        except Exception as e:
            print(f"Error downloading object '{key}': {e}")

    def upload_file(self, source: str, key: str):
        try:
            self.client.upload_file(source, self.bucket_name, key)
            print(f"Uploaded '{source}' to '{key}'")
        except Exception as e:
            print(f"Error uploading '{source}' to '{key}': {e}")
            raise

    def object_exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def get_object_stream(self, key: str):
        """
        Stream an object from S3/MinIO without downloading to disk.
        Returns a streaming body that can be wrapped with io.TextIOWrapper.
        Raises RuntimeError if the object does not exist or cannot be read.
        """
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            return response["Body"]
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in {"404", "NoSuchKey", "NotFound"}:
                raise RuntimeError(f"Object not found in bucket {self.bucket_name!r}: {key!r}") from e
            raise
