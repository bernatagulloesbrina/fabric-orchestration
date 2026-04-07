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
# META     },
# META     "warehouse": {
# META       "default_warehouse": "a5a12705-8c48-48ed-baef-8e490a9c0bee",
# META       "known_warehouses": [
# META         {
# META           "id": "a5a12705-8c48-48ed-baef-8e490a9c0bee",
# META           "type": "MountedWarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Load Fabric Items
# 
# Harvests workspaces, semantic models, dataflows (Gen2), and pipelines from the Fabric REST API
# and loads them into **Delta tables** in a Lakehouse.
# 
# **Setup (one-time):** After first run, attach "MetadataLakehouse" to this notebook in Fabric.

# CELL ********************

import sempy.fabric as fabric
import notebookutils
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Lakehouse name (will be created automatically if it doesn't exist)
LAKEHOUSE_NAME = 'MetadataLakehouse'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Setup: Create Lakehouse if needed
# 
# This cell creates the lakehouse if it doesn't exist. After running once, 
# **attach the lakehouse to this notebook** in Fabric:
# 1. Click "Add" in the left pane of this notebook
# 2. Select "Existing lakehouse"  
# 3. Choose "MetadataLakehouse"
# 4. Re-run the notebook

# CELL ********************

# Create lakehouse if it doesn't exist
try:
    lakehouse = notebookutils.lakehouse.get(LAKEHOUSE_NAME)
    print(f'✓ Lakehouse exists: {lakehouse.displayName} (ID: {lakehouse.id})')
except Exception:
    print(f'Creating lakehouse: {LAKEHOUSE_NAME}...')
    lakehouse = notebookutils.lakehouse.create(
        name=LAKEHOUSE_NAME,
        description="Metadata repository for Fabric workspaces, semantic models, dataflows, and pipelines"
    )
    print(f'✓ Created lakehouse: {lakehouse.displayName} (ID: {lakehouse.id})')
    print(f'\n⚠️  ACTION REQUIRED:')
    print(f'   1. Click "Add" in the left pane of this notebook')
    print(f'   2. Select "Existing lakehouse"')
    print(f'   3. Choose "{LAKEHOUSE_NAME}"')
    print(f'   4. Re-run this notebook\n')
    raise RuntimeError('Please attach the lakehouse and re-run the notebook')

# Verify lakehouse is attached (has default Spark context)
try:
    spark = SparkSession.builder.getOrCreate()
    spark.sql("SHOW DATABASES").collect()
    print(f'✓ Lakehouse is attached and ready')
except Exception as e:
    print(f'\n⚠️  Lakehouse not attached to notebook!')
    print(f'   Please attach "{LAKEHOUSE_NAME}" to this notebook:')
    print(f'   1. Click "Add" in the left pane')
    print(f'   2. Select "Existing lakehouse"')
    print(f'   3. Choose "{LAKEHOUSE_NAME}"\n')
    raise RuntimeError(f'Lakehouse attachment required')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fetch Fabric Artifacts from REST API

# CELL ********************

def fetch_all(client, url):
    """Calls a Fabric REST API endpoint and follows continuationUri pagination."""
    rows = []
    while url:
        resp = client.get(url)
        resp.raise_for_status()
        body = resp.json()
        rows.extend(body.get('value', []))
        url  = body.get('continuationUri')
    return rows

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

client       = fabric.FabricRestClient()
harvested_at = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

workspaces = fetch_all(client, '/v1/workspaces')
print(f'Workspaces found: {len(workspaces)}')

semantic_models, dataflows, pipelines = [], [], []

for ws in workspaces:
    ws_id = ws['id']

    for item in fetch_all(client, f'/v1/workspaces/{ws_id}/items?type=SemanticModel'):
        semantic_models.append((
            ws_id,
            item['id'],
            item.get('displayName', ''),
            item.get('description', ''),
        ))

    for item in fetch_all(client, f'/v1/workspaces/{ws_id}/items?type=Dataflow'):
        dataflows.append((ws_id, item['id'], item.get('displayName', '')))

    for item in fetch_all(client, f'/v1/workspaces/{ws_id}/items?type=DataPipeline'):
        pipelines.append((ws_id, item['id'], item.get('displayName', '')))

print(f'Semantic models : {len(semantic_models)}')
print(f'Dataflows (Gen2): {len(dataflows)}')
print(f'Pipelines       : {len(pipelines)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write to Delta Tables in Lakehouse
# 
# Converts the harvested data to Spark DataFrames and writes as Delta tables.
# Tables are automatically created or replaced in the attached lakehouse.

# CELL ********************

print(f'Writing tables to attached lakehouse...')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write workspaces table
workspaces_data = [
    (
        ws.get('id'),
        ws.get('displayName', ''),
        ws.get('type', ''),
        ws.get('state', ''),
        harvested_at
    )
    for ws in workspaces
]

workspaces_schema = StructType([
    StructField("workspace_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("state", StringType(), True),
    StructField("harvested_at", StringType(), True)
])

df_workspaces = spark.createDataFrame(workspaces_data, schema=workspaces_schema)
df_workspaces.write.format("delta").mode("overwrite").saveAsTable("workspaces")

print(f'✓ Wrote {len(workspaces)} workspaces')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write semantic_models table
semantic_models_schema = StructType([
    StructField("workspace_id", StringType(), False),
    StructField("item_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("description", StringType(), True),
    StructField("harvested_at", StringType(), True)
])

semantic_models_data = [
    (ws_id, item_id, display_name, description, harvested_at)
    for ws_id, item_id, display_name, description in semantic_models
]

df_semantic_models = spark.createDataFrame(semantic_models_data, schema=semantic_models_schema)
df_semantic_models.write.format("delta").mode("overwrite").saveAsTable("semantic_models")

print(f'✓ Wrote {len(semantic_models)} semantic models')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write dataflows table
dataflows_schema = StructType([
    StructField("workspace_id", StringType(), False),
    StructField("item_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("harvested_at", StringType(), True)
])

dataflows_data = [
    (ws_id, item_id, display_name, harvested_at)
    for ws_id, item_id, display_name in dataflows
]

df_dataflows = spark.createDataFrame(dataflows_data, schema=dataflows_schema)
df_dataflows.write.format("delta").mode("overwrite").saveAsTable("dataflows")

print(f'✓ Wrote {len(dataflows)} dataflows')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write pipelines table
pipelines_schema = StructType([
    StructField("workspace_id", StringType(), False),
    StructField("item_id", StringType(), False),
    StructField("display_name", StringType(), True),
    StructField("harvested_at", StringType(), True)
])

pipelines_data = [
    (ws_id, item_id, display_name, harvested_at)
    for ws_id, item_id, display_name in pipelines
]

df_pipelines = spark.createDataFrame(pipelines_data, schema=pipelines_schema)
df_pipelines.write.format("delta").mode("overwrite").saveAsTable("pipelines")

print(f'✓ Wrote {len(pipelines)} pipelines')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print('=' * 60)
print('✓ All artifact tables refreshed successfully!')
print('=' * 60)
print(f'\nYou can now query these tables:')
print(f'  - workspaces')
print(f'  - semantic_models')
print(f'  - dataflows')
print(f'  - pipelines')
print(f'\nExample: spark.sql("SELECT * FROM workspaces LIMIT 10").show()')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
