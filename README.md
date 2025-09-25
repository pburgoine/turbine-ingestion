# Basic project for ingesting Turbine Data using Databricks

## Basics

- Uses mise for handling tool dependencies.
- UV for python management.
- Databricks and pyspark for data work.
- Databricks Asset Bundles for deploying to Databricks.
- Ruff for formatting and linting.
- If you want to run this, add a `.env` file in the project with the following defined: 
    - `DATABRICKS_HOST=https://???.cloud.databricks.com/` - URL for your Databricks instance.
    - `BUNDLE_VAR_workspace_user=you@mail.com` - your email address for databricks account.
    - `BUNDLE_VAR_catalog=main` the Unity Catalog catalog you want to run this in eg `main`.
    - `BUNDLE_VAR_raw_turbine_data_path=/Volumes/main/landing/raw/` the location in databricks to read the raw data.
- Run `local_setup/setup.sh` in terminal to set up dependencies using mise.
- Run `eval "$(mise env)"` in the terminal to loads env variables.
- Use the mise tasks `db-deploy`, `db-destroy` and `db-destroy` to validate, destroy and deploy the Asset Bundle to Databricks.
- Use the `ruff-chk` and `ruff-fmt` to format and lint code.
- Run unit tests in pytest using `run-tests` mise task. 

## Databricks Data Solution

- Ingests data from a volume, due to files being appended to, uses `cloudFiles.allowOverwrites=true` to re ingest files.
- Loads raw data to bronze, with some extra metadata.
- Loads to silver, flagging any rows without turbine_id.
- Creates 2 gold aggregate tables at day and hour level.
- Creates an enriched silver table that flags anomalous rows using the gold data as lookup.
- Uses CDC flow to avoid duplicates in the silver enriched layer, which will use file modification time to update records.

## Considerations

- I attempted to use Declarative Pipelines in Databricks.
- Reuse spark functions where possible (compute_aggregates).
- Unit test functions using local pyspark and pytest (or in CICD), without relying on Databricks.
- Deploying in dev mode via asset bundles allows for isolated testing of pipelines as deploys resources with name end environment appended to job and environment as the schema.