import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

class ObjectStore:
    def __init__(self):
        self.endpoint = os.getenv("OBJECT_STORE_ENDPOINT", "http://localhost:9000")
        self.access_key = os.getenv("OBJECT_STORE_ACCESS_KEY", "minioadmin")
        self.secret_key = os.getenv("OBJECT_STORE_SECRET_KEY", "minioadmin")
        self.bucket_name = os.getenv("OBJECT_STORE_BUCKET_NAME", "sdpipe")

        self.client = boto3.client("s3", endpoint_url=self.endpoint, aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key, config=Config(signature_version="s3v4"), region_name="us-east-1")

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
