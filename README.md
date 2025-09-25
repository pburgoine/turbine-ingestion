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

- I attempted to use Declarative Pipelines in Databricks, done in a pipeline called `turbine_ingestion`.
- It ingests data from a volume, due to files being appended to, uses `cloudFiles.allowOverwrites=true` to re ingest files that are modified.
- It loads raw data to bronze, with some extra metadata.
- It then loads to silver, flagging any rows with `NULL` turbine_id or power_output.
- It creates 2 gold aggregate tables at day and hour level.
- It then creates an enriched silver table that flags anomalous rows using the gold data as lookup.
- Uses CDC flow to avoid duplicates in the silver enriched layer, which will use file modification time to update records.
- Workflow definition and declarative notebook are in the `resources` tab.

## Considerations

- Reuse spark functions where possible (compute_aggregates), and store these in python module that is imported to the notebook.
- Unit test functions using local pyspark and pytest (or in CICD), without relying on Databricks.
- Deploying in dev mode via asset bundles allows for isolated testing of pipelines as deploys resources with name end environment appended to job and environment as the schema.
- I do not think Declarative pipelines are a necessarily a good choice for Autoloader, however wanted to showcase declarative pipelines.
- I have modified the datasets and added extra data, and verified that the silver enriched table is properly deduplicated.
- I would have properly deduplicated the aggregate tables if I had time.