# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c1d1fc8c-6ef3-4d36-a24d-e81a3a472e8f",
# META       "default_lakehouse_name": "MetadataLakehouse",
# META       "default_lakehouse_workspace_id": "d1cce96c-953e-4c7e-8bc3-6f6e375f304c",
# META       "known_lakehouses": [
# META         {
# META           "id": "c1d1fc8c-6ef3-4d36-a24d-e81a3a472e8f"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Generate Jobs Table
# Creates a single unified table with all Fabric artifacts (semantic models, dataflows, pipelines, lakehouses, warehouses, etc.)
# including a generated jobName column in the format: workspaceName - objectType - objectName
# **Uses Fabric REST API (workspace identity compatible):**
# - `GET /v1/workspaces` - Returns all workspaces the identity has access to
# - `GET /v1/workspaces/{id}/items` - Returns all items in a workspace

# CELL ********************

import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Utility Functions
# 
# Column name normalization to snake_case (from UtilityFunctions notebook)

# CELL ********************

def normalize_column_name(name: str) -> str:
    """Convert a column name to valid snake_case format."""
    if not name or not isinstance(name, str):
        return 'col'
    
    # Preserve common meaningful symbols
    expanded = name.replace('%', ' pct ').replace('&', ' and ').replace('#', ' num ')
    
    # Convert to lowercase and replace non-alphanumeric with underscore
    lowered = expanded.lower()
    replaced = re.sub(r'[^a-z0-9]+', '_', lowered)
    
    # Collapse multiple underscores
    collapsed = replaced
    while '__' in collapsed:
        collapsed = collapsed.replace('__', '_')
    
    # Trim underscores and handle empty
    trimmed = collapsed.strip('_')
    if not trimmed:
        return 'col'
    
    # Column names shouldn't start with a digit
    if trimmed[0].isdigit():
        return 'c_' + trimmed
    
    return trimmed

def normalize_dataframe_columns(df):
    """Normalize all column names in a DataFrame with collision handling."""
    original_names = df.columns
    normalized_raw = [normalize_column_name(name) for name in original_names]
    
    # Handle duplicates by appending _2, _3, etc.
    seen_counts = {}
    final_names = []
    
    for norm_name in normalized_raw:
        if norm_name not in seen_counts:
            seen_counts[norm_name] = 1
            final_names.append(norm_name)
        else:
            seen_counts[norm_name] += 1
            final_names.append(f"{norm_name}_{seen_counts[norm_name]}")
    
    # Apply renames
    for old_name, new_name in zip(original_names, final_names):
        if old_name != new_name:
            df = df.withColumnRenamed(old_name, new_name)
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fetch All Workspaces

# CELL ********************

spark = SparkSession.builder.getOrCreate()

token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
headers = {"Authorization": f"Bearer {token}"}

def fetch_all(url):
    items = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        items.extend(data.get("value", []))
        url = data.get("continuationUri")
    return items

workspaces_raw = fetch_all("https://api.fabric.microsoft.com/v1/workspaces")
workspaces_df = pd.DataFrame([{"Id": w["id"], "Name": w["displayName"]} for w in workspaces_raw])
print(f'Workspaces found: {len(workspaces_df)}')
print(workspaces_df.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Collect All Items from All Workspaces
# Uses the Fabric REST API to fetch all items from each workspace.

# CELL ********************

# Collect all items from all workspaces
all_items = []

for _, workspace in workspaces_df.iterrows():
    ws_id = workspace['Id']
    ws_name = workspace['Name']
    
    try:
        items_raw = fetch_all(f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items")

        if items_raw:
            items_df = pd.DataFrame([
                {"Object Id": i["id"], "Display Name": i["displayName"], "Type": i["type"]}
                for i in items_raw
            ])
            items_df['Workspace Id'] = ws_id
            items_df['Workspace Name'] = ws_name
            all_items.append(items_df)
            print(f'{ws_name}: {len(items_df)} items')
    except Exception as e:
        print(f'Warning: Could not fetch items from {ws_name}: {e}')

# Combine all items into a single DataFrame
if all_items:
    all_items_df = pd.concat(all_items, ignore_index=True)
    print(f'\nTotal items collected: {len(all_items_df)}')
else:
    print('\nNo items found in any workspace')
    all_items_df = pd.DataFrame()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Fabic Items table with Generated Job Names
# 
# Converts the pandas DataFrame to Spark and adds a formatted jobName column.
# 
# **Key columns after normalization:**
# - `workspace_id` - Workspace GUID
# - `workspace_name` - Workspace display name
# - `object_id` - Item/object GUID
# - `type` - Item type (Lakehouse, Notebook, DataPipeline, etc.)
# - `display_name` - Item display name
# - `job_name` - Generated format: "WorkspaceName - Type - DisplayName"

# CELL ********************

if len(all_items_df) > 0:
    # Convert pandas DataFrame to Spark DataFrame
    df_spark = spark.createDataFrame(all_items_df)
    
    # Normalize column names to snake_case (fixes Delta table compatibility)
    df_spark = normalize_dataframe_columns(df_spark)
    
    # Create jobName column in format: "workspaceName - objectType - objectName"
    from pyspark.sql.functions import concat_ws, col
    
    df_jobs = df_spark.withColumn(
        'job_name',
        concat_ws(' - ', col('display_name'), col('type'), col('workspace_name'))
    )
    
    # Display preview
    print('\nFabric Items Table Preview:')
    df_jobs.select('workspace_id', 'workspace_name', 'object_id', 'type', 'display_name', 'job_name').show(10, truncate=False)
    
    print(f'\n✓ Generated {df_jobs.count()} job records')
else:
    df_jobs = None
    print('No data to process')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display summary by object type
if df_jobs:
    print('\nSummary by Object Type:')
    df_jobs.groupBy('type').count().orderBy('type').show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save to Delta Table
# 
# Saves the items table to the lakehouse as a Delta table.

# CELL ********************

if df_jobs:
    # Save as Delta table
    df_jobs.write.format("delta").mode("overwrite").saveAsTable("fabric_items")
    print('✓ Saved to Delta table: fabric_items')
    print(f'\nYou can now query this table with:')
    print(f'  SELECT * FROM fabric_items')
else:
    print('No data to save')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Extract Datasources for Refreshable Items
# # Builds a child table `refresh_job_sources` (one row per refreshable item × datasource)
# so we know what each semantic model / dataflow reads from (e.g. a SharePoint site).
# # **How it works:**
# - Reads the service principal credentials from `dbo.udf_config` in the **Metadata** Fabric SQL Database
#   (same keys the `triggerOnDemandRefresh` UDF uses).
# - Calls the **Power BI admin metadata scanner** (`admin/workspaces/getInfo` with `datasourceDetails=true`),
#   which returns dataset & dataflow datasources tenant-wide.
# - Joins back to `fabric_items` on `object_id` to attach the authoritative `job_name`.
# # **Prerequisites:**
# - The SP must be allowed to call read-only admin APIs / metadata scanning (Fabric Admin Portal →
#   *Admin API settings*: "Service principals can access read-only admin APIs" **and** "Enhanced metadata scanning").
# - Spark properties `spark.fabric.metadata.sql.server` and `spark.fabric.metadata.sql.database`
#   must be set on the attached Environment (see SETUP.md).

# CELL ********************

import json
import time

# Metadata SQL Database coordinates (set as Spark properties on the Environment; see SETUP.md).
def _required_conf(key: str) -> str:
    value = spark.conf.get(key, None)
    if not value:
        raise ValueError(
            f"Spark property '{key}' is not set. Attach the Environment with the Metadata SQL "
            f"properties to this notebook (see SETUP.md) before extracting datasources."
        )
    return value

def read_sp_config() -> dict:
    """Read service principal credentials from dbo.udf_config in the Metadata SQL Database.

    Uses the Spark connector for SQL databases (preinstalled in the Fabric runtime), which
    authenticates automatically with the notebook's running identity. That identity (the
    workspace identity when run from the pipeline) needs db_datareader on the Metadata DB.
    """
    sql_server = _required_conf("spark.fabric.metadata.sql.server")
    sql_database = _required_conf("spark.fabric.metadata.sql.database")
    url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};"

    wanted = ("SP_TENANT_ID", "SP_CLIENT_ID", "SP_CLIENT_SECRET")
    config_df = spark.read.option("url", url).mssql("dbo.udf_config")
    config = {
        row["config_key"]: row["config_value"]
        for row in config_df.filter(config_df.config_key.isin(list(wanted))).collect()
    }

    missing = [k for k in wanted if not config.get(k)]
    if missing:
        raise ValueError(f"Missing service principal config in dbo.udf_config: {missing}")
    return config

sp_config = read_sp_config()
print(f"Loaded {len(sp_config)} service principal config values from dbo.udf_config")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Acquire a Power BI token for the service principal via the client-credentials grant.
# Resource is analysis.windows.net/powerbi/api (NOT the Fabric resource used above).
def get_powerbi_token(config: dict) -> str:
    resp = requests.post(
        f"https://login.microsoftonline.com/{config['SP_TENANT_ID']}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": config["SP_CLIENT_ID"],
            "client_secret": config["SP_CLIENT_SECRET"],
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

ADMIN_BASE = "https://api.powerbi.com/v1.0/myorg/admin/workspaces"

def scan_workspaces(workspace_ids, pbi_headers):
    """Run the admin metadata scanner for up to 100 workspace ids; return the scan result json."""
    start_resp = requests.post(
        f"{ADMIN_BASE}/getInfo",
        params={
            "datasourceDetails": "true",
            "lineage": "false",
            "datasetSchema": "false",
            "datasetExpressions": "false",
            "getArtifactUsers": "false",
        },
        headers=pbi_headers,
        json={"workspaces": workspace_ids},
        timeout=60,
    )
    start_resp.raise_for_status()
    scan_id = start_resp.json()["id"]

    # Poll until the scan finishes.
    while True:
        status_resp = requests.get(f"{ADMIN_BASE}/scanStatus/{scan_id}", headers=pbi_headers, timeout=30)
        status_resp.raise_for_status()
        status = status_resp.json().get("status")
        if status == "Succeeded":
            break
        if status in ("Failed", "Disabled"):
            raise RuntimeError(f"Workspace scan {scan_id} ended with status '{status}'")
        time.sleep(2)

    result_resp = requests.get(f"{ADMIN_BASE}/scanResult/{scan_id}", headers=pbi_headers, timeout=60)
    result_resp.raise_for_status()
    return result_resp.json()

def flatten_connection(details):
    """Reduce a connectionDetails dict to a single friendly string."""
    if not isinstance(details, dict):
        return None
    if details.get("url"):
        return details["url"]
    server, database = details.get("server"), details.get("database")
    if server and database:
        return f"{server};{database}"
    if server:
        return server
    if details.get("path"):
        return details["path"]
    return json.dumps(details, sort_keys=True) if details else None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Scan all workspaces in batches and flatten dataset/dataflow datasources into rows.
# Collections map to the same 'type' values fabric_items uses, so the later join works.
ARTIFACT_TYPES = [("datasets", "SemanticModel"), ("dataflows", "Dataflow")]
SCAN_BATCH = 100

# Datasource extraction is additive: fabric_items is already saved, so a failure here
# (e.g. metadata scanning not enabled for the SP) degrades to an empty refresh_job_sources
# table with a clear warning instead of failing the items load.
source_rows = []
if df_jobs:
    try:
        pbi_headers = {"Authorization": f"Bearer {get_powerbi_token(sp_config)}"}
        workspace_ids = workspaces_df["Id"].tolist()
        print(f"Scanning {len(workspace_ids)} workspaces for datasource details...")

        for start in range(0, len(workspace_ids), SCAN_BATCH):
            batch_ids = workspace_ids[start:start + SCAN_BATCH]
            result = scan_workspaces(batch_ids, pbi_headers)

            # Root-level datasource instances (incl. misconfigured) keyed by datasourceId.
            instances = {
                di["datasourceId"]: di
                for di in (result.get("datasourceInstances", []) or [])
                + (result.get("misconfiguredDatasourceInstances", []) or [])
                if di.get("datasourceId")
            }

            for ws in result.get("workspaces", []) or []:
                for collection, object_type in ARTIFACT_TYPES:
                    for artifact in ws.get(collection, []) or []:
                        object_id = artifact.get("objectId") or artifact.get("id")
                        if not object_id:
                            continue
                        usages = (artifact.get("datasourceUsages", []) or []) \
                            + (artifact.get("misconfiguredDatasourceUsages", []) or [])
                        for usage in usages:
                            ds_id = usage.get("datasourceInstanceId")
                            di = instances.get(ds_id, {})
                            source_rows.append({
                                "object_id": object_id,
                                "object_type": object_type,
                                "datasource_type": di.get("datasourceType"),
                                "datasource_connection": flatten_connection(di.get("connectionDetails")),
                                "datasource_id": ds_id,
                            })

            print(f"  workspaces {start + 1}-{start + len(batch_ids)}: {len(source_rows)} source rows so far")

        print(f"\nCollected {len(source_rows)} datasource usage rows")
    except Exception as exc:
        source_rows = []
        print(f"⚠️  Datasource extraction failed ({type(exc).__name__}: {exc}).")
        print("    refresh_job_sources will be written empty. Check that the service principal")
        print("    has read-only admin API + enhanced metadata scanning enabled (see SETUP.md §6b).")
else:
    print("No items loaded; skipping datasource extraction")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Attach the authoritative job_name (join on object_id) and save refresh_job_sources.
from pyspark.sql.types import StructType, StructField, StringType

if df_jobs and source_rows:
    sources_schema = StructType([
        StructField("object_id", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("datasource_type", StringType(), True),
        StructField("datasource_connection", StringType(), True),
        StructField("datasource_id", StringType(), True),
    ])
    # De-duplicate identical (item, datasource) pairs before building the DataFrame.
    seen = set()
    unique_rows = []
    for r in source_rows:
        key = (r["object_id"], r["datasource_id"])
        if key not in seen:
            seen.add(key)
            unique_rows.append(r)

    df_sources_raw = spark.createDataFrame(unique_rows, schema=sources_schema)

    # Join on object_id to attach job_name; inner join drops sources for items not in fabric_items.
    df_sources = (
        df_sources_raw.join(
            df_jobs.select("object_id", "job_name"),
            on="object_id",
            how="inner",
        )
        .select(
            "job_name", "object_id", "object_type",
            "datasource_type", "datasource_connection", "datasource_id",
        )
    )

    print(f"refresh_job_sources rows after join: {df_sources.count()}")
    df_sources.show(10, truncate=False)

    df_sources.write.format("delta").mode("overwrite").saveAsTable("refresh_job_sources")
    print("✓ Saved to Delta table: refresh_job_sources")
else:
    # No sources found: write/replace an empty table so downstream reads stay valid.
    empty_schema = StructType([
        StructField("job_name", StringType(), True),
        StructField("object_id", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("datasource_type", StringType(), True),
        StructField("datasource_connection", StringType(), True),
        StructField("datasource_id", StringType(), True),
    ])
    spark.createDataFrame([], schema=empty_schema) \
        .write.format("delta").mode("overwrite").saveAsTable("refresh_job_sources")
    print("✓ Saved empty Delta table: refresh_job_sources (no datasources found)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
