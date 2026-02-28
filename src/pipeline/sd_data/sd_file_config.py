from dataclasses import dataclass

@dataclass(frozen=True)
class SdFileConfig:
    data_set_name: str