from dataclasses import dataclass

@dataclass(frozen=True)
class ObjectStoreConfig:
    endpoint: str
    access_key: str
    secret_key: str
    bucket_name: str

    def __post_init__(self):
        if not all(value.strip() for value in [self.endpoint, self.access_key, self.secret_key, self.bucket_name]):
            raise ValueError("All ObjectStoreConfig fields must be non-empty strings.")