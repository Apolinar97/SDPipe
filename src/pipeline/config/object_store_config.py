from dataclasses import dataclass

@dataclass(frozen=True)
class ObjectStoreConfig:
    bucket_name: str
    endpoint: str | None = None
    access_key: str | None = None
    secret_key: str | None = None
    region: str | None = None

    def __post_init__(self):
        if not self.bucket_name or not self.bucket_name.strip():
            raise ValueError("ObjectStoreConfig.bucket_name must be a non-empty string.")
        has_access = bool(self.access_key and self.access_key.strip())
        has_secret = bool(self.secret_key and self.secret_key.strip())
        if has_access != has_secret:
            raise ValueError(
                "ObjectStoreConfig requires all-or-nothing credentials: "
                "set both access_key and secret_key, or neither (for IAM role/default chain)."
            )
