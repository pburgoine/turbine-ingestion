# Basic project for ingesting Turbine Data using Databricks

- Uses mise for handling tool dependencies.
- UV for python management.
- Databricks and pyspark for data work.
- If you want to run this, add a `.env` file in the project with the following defined: 
    - `DATABRICKS_HOST=https://???.cloud.databricks.com/` - URL for your Databricks instance.
    - `BUNDLE_VAR_workspace_user=you@mail.com` - your email address for databricks account.
    - `BUNDLE_VAR_catalog` the Unity Catalog catalog you want to run this in.
- Run local_setup/setup.sh in terminal to set up dependencies.
- Use the mise tasks to validate, destroy and deploy the Asset Bundle to Databricks.
