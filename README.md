# SDPipe
End-to-End data pipeline for San Diego (SD) traffic collisions and weather.

## dbt setup
From `sdpipe_transforms/` with the project virtualenv activated:

```bash
dbt deps
dbt debug
dbt run
dbt test
```

The project depends on `dbt_utils`, which is declared in `sdpipe_transforms/packages.yml` and installed by `dbt deps`.

## Logging
- Shared logging module docs: `src/pipeline/LOGGING.md`
