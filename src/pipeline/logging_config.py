from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any


_RESERVED_RECORD_FIELDS = set(logging.makeLogRecord({}).__dict__.keys())


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _extract_extra_fields(record: logging.LogRecord) -> dict[str, Any]:
    extras: dict[str, Any] = {}
    for key, value in record.__dict__.items():
        if key in _RESERVED_RECORD_FIELDS or key.startswith("_"):
            continue
        extras[key] = value
    return extras


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "service": self.service,
            "message": record.getMessage(),
        }

        extras = _extract_extra_fields(record)
        if extras:
            payload["context"] = extras

        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


class TextFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__(datefmt="%Y-%m-%dT%H:%M:%S%z")
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
        message = (
            f"{timestamp} {record.levelname} {record.name} service={self.service} "
            f"message={record.getMessage()}"
        )

        extras = _extract_extra_fields(record)
        if extras:
            extra_bits = " ".join(f"{key}={value!r}" for key, value in sorted(extras.items()))
            message = f"{message} {extra_bits}"

        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        return message


def configure_logging(level: str | None = None, service: str = "sdpipe") -> None:
    env_level = level or os.getenv("LOG_LEVEL", "INFO")
    resolved_level = getattr(logging, env_level.upper(), logging.INFO)
    aws_runtime = bool(os.getenv("AWS_LAMBDA_FUNCTION_NAME"))
    use_json = _parse_bool(os.getenv("LOG_JSON"), default=aws_runtime)

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter(service=service) if use_json else TextFormatter(service=service))

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(resolved_level)
    logging.captureWarnings(True)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def with_context(logger: logging.Logger, **context: Any) -> logging.LoggerAdapter:
    return logging.LoggerAdapter(logger, extra=context)
