from pipeline.staging.data_config import STAGING_DATASETS

for dataset in STAGING_DATASETS:
    print(dataset.name, dataset.table_name)