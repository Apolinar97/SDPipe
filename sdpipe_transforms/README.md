# sdpipe_transforms

dbt project for SDPipe collision models.

## Setup
From this directory with the project virtualenv activated:

```bash
dbt deps
dbt debug
```

`dbt deps` is required because this project uses `dbt_utils`, declared in `packages.yml`.

## Common commands
Run the full project:

```bash
dbt run
dbt test
```

Run the collision models touched by the participant identity work:

```bash
dbt run --select stg_collisions_basic stg_collisions_details int_collisions_basic int_collisions_details audit_collisions_details_repeated_rows
dbt test --select stg_collisions_basic stg_collisions_details int_collisions_basic int_collisions_details audit_collisions_details_repeated_rows test_int_collisions_details_preserves_staging_rows
```
