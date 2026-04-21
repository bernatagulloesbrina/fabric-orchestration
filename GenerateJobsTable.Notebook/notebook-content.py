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
# 
# Creates a single unified table with all Fabric artifacts (semantic models, dataflows, and pipelines)
# including a generated jobName column in the format: workspaceName - objectType - objectName

# CELL ********************

import sempy.fabric as fabric
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timezone

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

client = fabric.FabricRestClient()
spark = SparkSession.builder.getOrCreate()

workspaces = fetch_all(client, '/v1/workspaces')
print(f'Workspaces found: {len(workspaces)}')

# Create workspace lookup dictionary
workspace_lookup = {ws['id']: ws.get('displayName', '') for ws in workspaces}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Collect All Artifacts
# 
# Fetches semantic models, dataflows, and pipelines from all workspaces and combines them into a single list.

# CELL ********************

# Collect all artifacts with their type
jobs = []

for ws in workspaces:
    ws_id = ws['id']
    ws_name = ws.get('displayName', '')

    # Fetch Semantic Models
    for item in fetch_all(client, f'/v1/workspaces/{ws_id}/items?type=SemanticModel'):
        object_name = item.get('displayName', '')
        jobs.append((
            ws_id,
            ws_name,
            'SemanticModel',
            item['id'],
            object_name,
            f'{ws_name} - SemanticModel - {object_name}'
        ))

    # Fetch Dataflows (Gen2)
    for item in fetch_all(client, f'/v1/workspaces/{ws_id}/items?type=Dataflow'):
        object_name = item.get('displayName', '')
        jobs.append((
            ws_id,
            ws_name,
            'Dataflow',
            item['id'],
            object_name,
            f'{ws_name} - Dataflow - {object_name}'
        ))

    # Fetch Data Pipelines
    for item in fetch_all(client, f'/v1/workspaces/{ws_id}/items?type=DataPipeline'):
        object_name = item.get('displayName', '')
        jobs.append((
            ws_id,
            ws_name,
            'DataPipeline',
            item['id'],
            object_name,
            f'{ws_name} - DataPipeline - {object_name}'
        ))

print(f'Total artifacts collected: {len(jobs)}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Create Jobs Table
# 
# Generates a single DataFrame with all artifacts and the formatted jobName column.

# CELL ********************

# Define schema
jobs_schema = StructType([
    StructField("workspaceId", StringType(), False),
    StructField("workspaceName", StringType(), True),
    StructField("objectType", StringType(), False),
    StructField("objectId", StringType(), False),
    StructField("objectName", StringType(), True),
    StructField("jobName", StringType(), True)
])

# Create DataFrame
df_jobs = spark.createDataFrame(jobs, schema=jobs_schema)

# Display preview
print('\nJobs Table Preview:')
df_jobs.show(10, truncate=False)

print(f'\n✓ Generated {df_jobs.count()} job records')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Display summary by object type
print('\nSummary by Object Type:')
df_jobs.groupBy('objectType').count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save to Delta Table (Optional)
# 
# Uncomment the code below to save the table to a lakehouse.

# CELL ********************

# Uncomment to save as Delta table:
# df_jobs.write.format("delta").mode("overwrite").saveAsTable("fabric_jobs")
# print('✓ Saved to Delta table: fabric_jobs')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
