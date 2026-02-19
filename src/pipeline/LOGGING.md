# SDPipe Logging

This project uses a shared logging module at `src/pipeline/logging_config.py`.

## Goals
- One logging setup for modules, scripts, and Lambda handlers.
- Logs written to `stdout` so they work locally and in AWS CloudWatch.
- Consistent log format across the codebase.

## Public API
- `configure_logging(level: str | None = None, service: str = "sdpipe") -> None`
- `get_logger(name: str) -> logging.Logger`
- `with_context(logger: logging.Logger, **context) -> logging.LoggerAdapter`

## Runtime Behavior
- Local development default: text logs.
- AWS Lambda default: JSON logs (auto-detected via `AWS_LAMBDA_FUNCTION_NAME`).
- Override JSON/text explicitly with `LOG_JSON`.

## Environment Variables
- `LOG_LEVEL`
  - Default: `INFO`
  - Typical values: `DEBUG`, `INFO`, `WARNING`, `ERROR`
- `LOG_JSON`
  - `true`/`false` (case-insensitive)
  - If unset, defaults to JSON in Lambda and text locally

## Usage Pattern

### In a module
```python
from pipeline.logging_config import get_logger

logger = get_logger(__name__)

def run() -> None:
    logger.info("Processing started")
```

### In an entrypoint (script/Lambda/main)
```python
import os
from pipeline.logging_config import configure_logging, get_logger

logger = get_logger(__name__)

def main() -> None:
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"), service="pipeline.example")
    logger.info("Entrypoint initialized")
```

### Add structured context
```python
from pipeline.logging_config import get_logger, with_context

logger = with_context(get_logger(__name__), station_id="KSAN", run_date="2026-02-19")
logger.info("Fetched observation")
```

## Conventions
- Prefer `logger.info(...)`, `logger.warning(...)`, `logger.error(...)`, `logger.exception(...)`.
- Do not use `print(...)` for operational logs.
- Do not swallow exceptions with `pass`; log the failure with useful context.
- Include identifiers in messages/context (`station_id`, `dataset`, `key`, `run_date`).

## Local Examples
```bash
# Text logs (default outside Lambda)
LOG_LEVEL=DEBUG PYTHONPATH=src ./venv/bin/python -m pipeline.staging.load_staging

# Force JSON logs locally
LOG_LEVEL=INFO LOG_JSON=true PYTHONPATH=src ./venv/bin/python -m pipeline.staging.load_staging
```

## CloudWatch Notes
- Lambda automatically captures `stdout`/`stderr` into CloudWatch Logs.
- JSON logs emitted by this module are easy to filter and parse in CloudWatch Logs Insights.
