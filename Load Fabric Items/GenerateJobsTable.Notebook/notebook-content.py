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

# PARAMETERS CELL ********************

# Service principal credentials for the admin metadata scanner, injected by the
# "Load Fabric Items" pipeline via a Lookup on dbo.udf_config. Left empty for interactive
# runs, where the notebook falls back to the signed-in user's identity (getToken("pbi")).
sp_tenant_id = ""
sp_client_id = ""
sp_client_secret = ""

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

def get_fabric_token():
    """Fabric API token: the service principal (client-credentials) when supplied by the
    pipeline, else the running identity. The SP path lets headless runs use the admin APIs."""
    if sp_tenant_id and sp_client_id and sp_client_secret:
        resp = requests.post(
            f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": sp_client_id,
                "client_secret": sp_client_secret,
                "scope": "https://api.fabric.microsoft.com/.default",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]
    return notebookutils.credentials.getToken("https://api.fabric.microsoft.com")

# With an SP we use the tenant-wide admin APIs; otherwise the member-scoped APIs (which only
# see workspaces the running identity belongs to -- just 1 for the workspace identity headless).
use_admin_apis = bool(sp_tenant_id and sp_client_id and sp_client_secret)
token = get_fabric_token()
headers = {"Authorization": f"Bearer {token}"}

def fetch_all(url, list_keys=("value",)):
    items = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        batch = next((data[k] for k in list_keys if data.get(k) is not None), [])
        items.extend(batch)
        url = data.get("continuationUri")
    return items

if use_admin_apis:
    workspaces_raw = fetch_all("https://api.fabric.microsoft.com/v1/admin/workspaces",
                               list_keys=("workspaces", "value"))
    # Skip personal "My workspaces" to match the member-scoped catalogue.
    workspaces_df = pd.DataFrame([
        {"Id": w["id"], "Name": w.get("name") or w.get("displayName")}
        for w in workspaces_raw if w.get("type") != "Personal"
    ])
else:
    workspaces_raw = fetch_all("https://api.fabric.microsoft.com/v1/workspaces")
    workspaces_df = pd.DataFrame([{"Id": w["id"], "Name": w["displayName"]} for w in workspaces_raw])

print(f'Workspaces found: {len(workspaces_df)} ({"admin" if use_admin_apis else "member"}-scoped)')
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

if use_admin_apis:
    # One tenant-wide admin call returns every item with its workspaceId (paginated).
    ws_name_by_id = dict(zip(workspaces_df["Id"], workspaces_df["Name"]))
    items_raw = fetch_all("https://api.fabric.microsoft.com/v1/admin/items",
                          list_keys=("itemEntities", "value"))
    all_items_df = pd.DataFrame([
        {
            "Object Id": i["id"],
            "Display Name": i.get("name") or i.get("displayName"),
            "Type": i["type"],
            "Workspace Id": i["workspaceId"],
            "Workspace Name": ws_name_by_id.get(i["workspaceId"]),
        }
        for i in items_raw
        if i.get("workspaceId") in ws_name_by_id  # keep items in catalogued (non-personal) workspaces
    ])
    print(f'Total items collected: {len(all_items_df)} (admin, tenant-wide)')
else:
    # Member-scoped: fetch items per workspace the running identity belongs to.
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
# - Calls the **Power BI admin metadata scanner** (`admin/workspaces/getInfo` with `datasourceDetails=true`),
#   which returns dataset & dataflow datasources tenant-wide, authenticating with the notebook's
#   own identity token (`notebookutils.credentials.getToken("pbi")`).
# - Joins back to `fabric_items` on `object_id` to attach the authoritative `job_name`.
# # **Prerequisites — the identity running this notebook must be allowed to call the read-only
# admin APIs and metadata scanning** (Fabric Admin Portal -> *Admin API settings*:
# "Service principals can access read-only admin APIs" AND "Enhanced metadata scanning"):
# - Interactive runs use *your* identity (works if you are a Fabric admin).
# - Pipeline runs use the **workspace identity** -- add it to the allowed security group for the
#   two settings above. If that token is rejected for the admin APIs, switch to an explicit
#   service principal (Key Vault secret + client-credentials grant); see SETUP.md section 6b.


# CELL ********************

import json
import time

ADMIN_BASE = "https://api.powerbi.com/v1.0/myorg/admin/workspaces"

def get_pbi_token():
    """Power BI token for the admin scanner.

    Uses the service principal (client-credentials) when the pipeline supplies credentials;
    otherwise falls back to the signed-in identity. The SP path is what makes headless pipeline
    runs work -- the workspace identity is not authorized for the read-only admin APIs.
    """
    if sp_tenant_id and sp_client_id and sp_client_secret:
        resp = requests.post(
            f"https://login.microsoftonline.com/{sp_tenant_id}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": sp_client_id,
                "client_secret": sp_client_secret,
                "scope": "https://analysis.windows.net/powerbi/api/.default",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]
    return notebookutils.credentials.getToken("pbi")

def get_scan_workspace_ids(pbi_headers, fallback_ids):
    """All workspace ids to scan, enumerated via the admin API (tenant-wide).

    The member-scoped /v1/workspaces list only returns workspaces the running identity belongs to
    (just 1 for the workspace identity in a pipeline), so enumerate via the admin API instead.
    Falls back to the member-scoped list if the admin call isn't permitted.
    """
    try:
        resp = requests.get(f"{ADMIN_BASE}/modified", headers=pbi_headers, timeout=60)
        resp.raise_for_status()
        ids = [w["id"] for w in resp.json() if w.get("id")]
        return ids or fallback_ids
    except Exception as exc:
        print(f"  admin workspace enumeration failed ({type(exc).__name__}: {exc}); "
              f"falling back to member-scoped workspace list")
        return fallback_ids

def scan_workspaces(workspace_ids, pbi_headers):
    """Run the admin metadata scanner for up to 100 workspace ids; return the scan result json."""
    start_resp = requests.post(
        f"{ADMIN_BASE}/getInfo",
        params={
            # datasourceDetails -> datasourceInstances[] (connection details);
            # lineage -> datasourceUsages[] on each artifact (links artifact -> instance).
            # Both are required to map a dataset/dataflow to its data sources.
            "datasourceDetails": "true",
            "lineage": "true",
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
    # Single-value connection fields, in order of preference.
    for key in ("url", "sharePointSiteUrl", "path"):
        if details.get(key):
            return details[key]
    server, database = details.get("server"), details.get("database")
    if server and database:
        return f"{server};{database}"
    if server:
        return server
    return json.dumps(details, sort_keys=True) if details else None

def describe_datasource(di):
    """Return (datasource_type, datasource_connection) from a scanner datasource instance.

    'Extension' is the generic wrapper for Fabric-native sources; the concrete kind
    (Lakehouse, Warehouse, Notebook, FabricSql, ...) lives in extensionDataSourceKind.
    A semantic model on an Extension/Lakehouse (or /Warehouse) source is Direct Lake.
    Note: the scanner only reports the generic kind, not which specific lakehouse/warehouse.
    """
    details = di.get("connectionDetails") or {}
    dstype = di.get("datasourceType")
    if dstype == "Extension":
        kind = details.get("extensionDataSourceKind")
        path = details.get("extensionDataSourcePath")
        dstype = kind or "Extension"
        # extensionDataSourcePath is usually just the kind again; only surface it if it adds info.
        conn = path if (path and path != kind) else kind
        return dstype, conn
    return dstype, flatten_connection(details)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Scan all workspaces in batches and flatten dataset/dataflow datasources into rows.
# The scanner groups artifacts two ways: classic lowercase plural keys (datasets, dataflows = Gen1)
# and capitalized Fabric item-type keys (Dataflow = Gen2 / CI-CD, which use the Fabric item id so
# they join cleanly to fabric_items). Iterate both; dedup on (object_id, datasource_id) below.
ARTIFACT_TYPES = [
    ("datasets", "SemanticModel"),
    ("dataflows", "Dataflow"),
    ("Dataflow", "Dataflow"),
]
SCAN_BATCH = 100

# Datasource extraction is additive: fabric_items is already saved, so a failure here
# (e.g. metadata scanning not enabled, or the identity not authorized) degrades to an empty
# refresh_job_sources table with a clear warning instead of failing the items load.
source_rows = []
if df_jobs:
    try:
        pbi_headers = {"Authorization": f"Bearer {get_pbi_token()}"}
        workspace_ids = get_scan_workspace_ids(pbi_headers, workspaces_df["Id"].tolist())
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
                ws_name = ws.get("name")
                for collection, object_type in ARTIFACT_TYPES:
                    for artifact in ws.get(collection, []) or []:
                        object_id = artifact.get("objectId") or artifact.get("id")
                        object_name = artifact.get("name")
                        if not object_id or not object_name:
                            continue
                        # Build job_name matching fabric_items ("display_name - type - workspace_name").
                        # Used as a fallback for items whose scanner id != Fabric item id (e.g. dataflows);
                        # semantic models still get the canonical job_name via the object_id join below.
                        job_name_fallback = f"{object_name} - {object_type} - {ws_name}"
                        usages = (artifact.get("datasourceUsages", []) or []) \
                            + (artifact.get("misconfiguredDatasourceUsages", []) or [])
                        for usage in usages:
                            ds_id = usage.get("datasourceInstanceId")
                            di = instances.get(ds_id, {})
                            ds_type, ds_conn = describe_datasource(di)
                            source_rows.append({
                                "object_id": object_id,
                                "object_type": object_type,
                                "job_name_fallback": job_name_fallback,
                                "datasource_type": ds_type,
                                "datasource_connection": ds_conn,
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

# Resolve job_name and save refresh_job_sources.
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import coalesce, col

if df_jobs and source_rows:
    sources_schema = StructType([
        StructField("object_id", StringType(), True),
        StructField("object_type", StringType(), True),
        StructField("job_name_fallback", StringType(), True),
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

    # Left-join to fabric_items on object_id: semantic models get the canonical job_name,
    # dataflows (scanner id != Fabric item id) fall back to the job_name built from the scan.
    df_sources = (
        df_sources_raw.join(
            df_jobs.select("object_id", col("job_name").alias("fi_job_name")),
            on="object_id",
            how="left",
        )
        .withColumn("job_name", coalesce("fi_job_name", "job_name_fallback"))
        .select(
            "job_name", "object_id", "object_type",
            "datasource_type", "datasource_connection", "datasource_id",
        )
    )

    print(f"refresh_job_sources rows: {df_sources.count()}")
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
