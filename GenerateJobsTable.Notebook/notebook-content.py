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
# Creates a single unified table with all Fabric artifacts (semantic models, dataflows, pipelines, lakehouses, warehouses, etc.)
# including a generated jobName column in the format: workspaceName - objectType - objectName
# 
# **Uses sempy.fabric methods:**
# - `list_workspaces()` - Returns all workspaces as a pandas DataFrame
# - `list_items(workspace)` - Returns all items in a workspace as a pandas DataFrame

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

# ## Fetch All Workspaces

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# Get all workspaces using sempy's built-in method (returns a pandas DataFrame)
workspaces_df = fabric.list_workspaces()
print(f'Workspaces found: {len(workspaces_df)}')
print(workspaces_df.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Collect All Items from All Workspaces
# 
# Uses sempy's list_items() method to fetch all items from each workspace.

# CELL ********************

# Collect all items from all workspaces
all_items = []

for _, workspace in workspaces_df.iterrows():
    ws_id = workspace['Id']
    ws_name = workspace['Name']
    
    try:
        # Get all items in this workspace (returns pandas DataFrame)
        items_df = fabric.list_items(workspace=ws_id)
        
        if len(items_df) > 0:
            # Add workspace info to each item
            items_df['Workspace Id'] = ws_id
            items_df['Workspace Name'] = ws_name
            all_items.append(items_df)
            print(f'{ws_name}: {len(items_df)} items')
    except Exception as e:
        print(f'Warning: Could not fetch items from {ws_name}: {e}')

# Combine all items into a single DataFrame
if all_items:
    import pandas as pd
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

# ## Create Jobs Table with Generated Job Names
# 
# Converts the pandas DataFrame to Spark and adds a formatted jobName column.

# CELL ********************

if len(all_items_df) > 0:
    # Convert pandas DataFrame to Spark DataFrame
    df_spark = spark.createDataFrame(all_items_df)
    
    # Create jobName column in format: "workspaceName - objectType - objectName"
    from pyspark.sql.functions import concat_ws, col
    
    df_jobs = df_spark.withColumn(
        'jobName',
        concat_ws(' - ', col('Workspace Name'), col('Type'), col('Display Name'))
    )
    
    # Display preview
    print('\nFabric Items Table Preview:')
    df_jobs.select('Workspace Name', 'Type', 'Display Name', 'jobName').show(10, truncate=False)
    
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
    df_jobs.groupBy('Type').count().orderBy('Type').show()

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
