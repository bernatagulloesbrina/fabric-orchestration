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
# **Zero configuration required!** The Lakehouse is created automatically if it doesn't exist.

# CELL ********************

import sempy.fabric as fabric
import notebookutils
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timezone

# PARAMETERS CELL ********************

# Lakehouse name - will be created if it doesn't exist
LAKEHOUSE_NAME = 'MetadataLakehouse'

# Database name for Delta tables (will be created if it doesn't exist)
DATABASE_NAME = 'metadata'

# MARKDOWN ********************

# ## Setup: Create Lakehouse if needed

# CELL ********************

# Check if lakehouse exists, create if needed
try:
    lakehouse = notebookutils.lakehouse.get(LAKEHOUSE_NAME)
    print(f'✓ Using existing lakehouse: {lakehouse.displayName}')
except Exception:
    # Lakehouse doesn't exist, create it
    print(f'Creating lakehouse: {LAKEHOUSE_NAME}...')
    lakehouse = notebookutils.lakehouse.create(
        name=LAKEHOUSE_NAME,
        description="Metadata repository for Fabric workspaces, semantic models, dataflows, and pipelines"
    )
    print(f'✓ Created lakehouse: {lakehouse.displayName} (ID: {lakehouse.id})')

# Attach the lakehouse to this notebook for easy access
try:
    notebookutils.notebook.attachLakehouse(lakehouse.id)
    print(f'✓ Lakehouse attached to notebook')
except Exception as e:
    print(f'Note: Lakehouse attachment: {e}')

# MARKDOWN ********************

# ## Fetch Fabric Artifacts

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

# ## Load data into SQL Database
# 
# Uses token authentication - no password needed!
#
# **ONE-TIME SETUP**: Update the server name in `get_sql_connection_from_workspace()` below

# CELL ********************

# Connect to SQL Database using session token
connection = get_sql_connection_from_workspace(SQL_DATABASE_NAME)
cursor = connection.cursor()

# Verify connection and database
cursor.execute("SELECT DB_NAME() AS CurrentDatabase")
current_db = cursor.fetchone()[0]
print(f'Connected to database: {current_db}')

# Check if tables exist
cursor.execute("""
    SELECT TABLE_SCHEMA, TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = 'dbo' 
    AND TABLE_NAME IN ('workspaces', 'semantic_models', 'dataflows', 'pipelines')
    ORDER BY TABLE_NAME
""")
tables = cursor.fetchall()
print(f'Found {len(tables)} metadata tables:')
for schema, table in tables:
    print(f'  - {schema}.{table}')

if len(tables) < 4:
    raise RuntimeError(f'Expected 4 tables but found {len(tables)}. Please run the table creation script first.')

# Helper function to escape single quotes for SQL
def escape_sql(value):
    "Write to Delta Tables in Lakehouse
# 
# Converts the harvested data to Spark DataFrames and writes as Delta tables.
# Tables are automatically created or replaced - **no setup required!**

# CELL ********************

# Get Spark session
spark = SparkSession.builder.getOrCreate()

# Create database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}")
spark.sql(f"USE {DATABASE_NAME}")

print(f'Using database: {DATABASE_NAME}')

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
df_workspaces.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE_NAME}.workspaces")

print(f'✓ Wrote {len(workspaces)} workspaces to {DATABASE_NAME}.workspaces')

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
df_semantic_models.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE_NAME}.semantic_models")

print(f'✓ Wrote {len(semantic_models)} semantic models to {DATABASE_NAME}.semantic_models')

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
df_dataflows.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE_NAME}.dataflows")

print(f'✓ Wrote {len(dataflows)} dataflows to {DATABASE_NAME}.dataflows')

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
df_pipelines.write.format("delta").mode("overwrite").saveAsTable(f"{DATABASE_NAME}.pipelines")

print(f'✓ Wrote {len(pipelines)} pipelines to {DATABASE_NAME}.pipelines')

# CELL ********************

print('=' * 60)
print('✓ All artifact tables refreshed successfully!')
print('=' * 60)
print(f'\nYou can now query these tables using SQL or Spark:')
print(f'  - {DATABASE_NAME}.workspaces')
print(f'  - {DATABASE_NAME}.semantic_models')
print(f'  - {DATABASE_NAME}.dataflows')
print(f'  - {DATABASE_NAME}.pipelines')
print(f'\nExample: spark.sql("SELECT * FROM {DATABASE_NAME}.workspaces").show()