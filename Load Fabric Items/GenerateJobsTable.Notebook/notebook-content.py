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
            # Rename 'Id' to 'Object Id' for clarity
            if 'Id' in items_df.columns:
                items_df = items_df.rename(columns={'Id': 'Object Id'})
            
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
