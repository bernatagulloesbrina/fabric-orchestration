# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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

# PARAMETERS CELL ********************

# Lakehouse name (will be created automatically if it doesn't exist)
LAKEHOUSE_NAME = 'MetadataLakehouse'

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

# MARKDOWN ********************

# ## Write to Delta Tables in Lakehouse
# 
# Converts the harvested data to Spark DataFrames and writes as Delta tables.
# Tables are automatically created or replaced in the attached lakehouse.

# CELL ********************

print(f'Writing tables to attached lakehouse...')

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
