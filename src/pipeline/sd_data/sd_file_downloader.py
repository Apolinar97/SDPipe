import os
from datetime import date

import requests

from pipeline.config.object_store_env import get_object_store_config
from pipeline.logging_config import configure_logging, get_logger
from pipeline.sd_data.sd_file_config import SdFileConfig, SdFileManagerConfig
from pipeline.storage.object_store import ObjectStore

logger = get_logger(__name__)


def _fetch_folder_date(files: list[SdFileConfig]) -> date:
    """Return the max source Last-Modified date across all files."""
    source_dates = []
    for config in files:
        try:
            source_dates.append(config.get_source_last_modified().date())
        except Exception:
            logger.exception("Failed to get source Last-Modified: dataset=%s", config.data_set_name)
    if not source_dates:
        raise RuntimeError("Could not determine source Last-Modified for any file")
    return max(source_dates)


def _download_to_store(config: SdFileConfig, store: ObjectStore, folder_date: date) -> bool:
    """Download a single file to object store. Returns True if downloaded, False if skipped."""
    key = config.get_object_key(folder_date)
    if store.object_exists(key):
        logger.info("Skipping, already exists: dataset=%s key=%s", config.data_set_name, key)
        return False
    source_last_modified = config.get_source_last_modified()
    logger.info("Downloading: dataset=%s url=%s", config.data_set_name, config.url)
    response = requests.get(config.url, stream=True, timeout=60)
    response.raise_for_status()
    data = response.content
    store.put_object(
        key=key,
        data=data,
        content_type="text/csv",
        metadata={"source-last-modified": source_last_modified.isoformat()},
    )
    logger.info("Stored: dataset=%s key=%s bytes=%d", config.data_set_name, key, len(data))
    return True


def main() -> None:
    configure_logging(level=os.getenv("LOG_LEVEL", "INFO"), service="pipeline.sd_data")
    store = ObjectStore(get_object_store_config("AWS_S3_BUCKET_NAME"))
    manager = SdFileManagerConfig()
    files = manager.all_files()
    logger.info("Starting SD data fetch: files=%d bucket=%s", len(files), store.bucket_name)
    folder_date = _fetch_folder_date(files)
    logger.info("Folder date (max source Last-Modified): %s", folder_date)
    downloaded, skipped, failed = 0, 0, []
    for config in files:
        try:
            if _download_to_store(config, store, folder_date):
                downloaded += 1
            else:
                skipped += 1
        except Exception:
            logger.exception("Failed to download: dataset=%s", config.data_set_name)
            failed.append(config.data_set_name)
    logger.info("SD data fetch complete: downloaded=%d skipped=%d failed=%d", downloaded, skipped, len(failed))
    if failed:
        raise RuntimeError(f"SD data fetch failed for {len(failed)} file(s): {failed}")


if __name__ == "__main__":
    main()
