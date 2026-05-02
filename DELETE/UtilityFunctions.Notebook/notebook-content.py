# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Utility Functions
# 
# Reusable helper functions for Fabric notebooks.
# 
# **Available functions:**
# - `normalize_column_name(name)` - Convert a single column name to snake_case
# - `normalize_dataframe_columns(df)` - Normalize all column names in a Spark or Pandas DataFrame

# CELL ********************

import re
from typing import Union
from pyspark.sql import DataFrame as SparkDataFrame

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Snake Case Normalization
# 
# Converts column names to valid snake_case format, handling:
# - Special characters (%, &, #)
# - Non-alphanumeric characters
# - Leading digits
# - Duplicate names
# - Empty names

# CELL ********************

def normalize_column_name(name: str) -> str:
    """
    Convert a column name to snake_case following these rules:
    1. Replace common symbols with words (%, &, #)
    2. Convert to lowercase
    3. Replace non-alphanumeric with underscore
    4. Collapse multiple underscores
    5. Trim underscores from edges
    6. Handle empty names and leading digits
    
    Examples:
        'Display Name' -> 'display_name'
        'Sales %' -> 'sales_pct'
        'Q&A' -> 'q_and_a'
        '2024 Revenue' -> 'c_2024_revenue'
    """
    if not name or not isinstance(name, str):
        return 'col'
    
    # Step 1: Preserve common meaningful symbols before stripping
    expanded = name.replace('%', ' pct ').replace('&', ' and ').replace('#', ' num ')
    
    # Step 2: Convert to lowercase
    lowered = expanded.lower()
    
    # Step 3: Replace any character outside [a-z0-9] with "_"
    # Using regex for efficiency
    replaced = re.sub(r'[^a-z0-9]+', '_', lowered)
    
    # Step 4: Collapse runs of underscores (already handled by regex with +)
    # But let's ensure no double underscores remain
    collapsed = replaced
    while '__' in collapsed:
        collapsed = collapsed.replace('__', '_')
    
    # Step 5: Trim underscores from start and end
    trimmed = collapsed.strip('_')
    
    # Step 6: Handle empty result
    if not trimmed:
        return 'col'
    
    # Step 7: Column names shouldn't start with a digit
    if trimmed[0].isdigit():
        return 'c_' + trimmed
    
    return trimmed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_dataframe_columns(df: Union[SparkDataFrame, 'pd.DataFrame']) -> Union[SparkDataFrame, 'pd.DataFrame']:
    """
    Normalize all column names in a DataFrame to snake_case with collision handling.
    Supports both Spark and Pandas DataFrames.
    
    Args:
        df: A Spark or Pandas DataFrame
        
    Returns:
        DataFrame with normalized column names
        
    Examples:
        df_clean = normalize_dataframe_columns(df)
    """
    import pandas as pd
    
    is_spark = isinstance(df, SparkDataFrame)
    
    # Get original column names
    if is_spark:
        original_names = df.columns
    else:
        original_names = df.columns.tolist()
    
    # Normalize all names
    normalized_raw = [normalize_column_name(name) for name in original_names]
    
    # Handle duplicates by appending _2, _3, etc.
    seen_counts = {}
    final_names = []
    
    for norm_name in normalized_raw:
        if norm_name not in seen_counts:
            # First occurrence
            seen_counts[norm_name] = 1
            final_names.append(norm_name)
        else:
            # Collision - append counter
            seen_counts[norm_name] += 1
            final_names.append(f"{norm_name}_{seen_counts[norm_name]}")
    
    # Create rename mapping
    rename_map = dict(zip(original_names, final_names))
    
    # Apply renames
    if is_spark:
        # Spark DataFrame
        for old_name, new_name in rename_map.items():
            if old_name != new_name:
                df = df.withColumnRenamed(old_name, new_name)
    else:
        # Pandas DataFrame
        df = df.rename(columns=rename_map)
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Test Examples

# CELL ********************

# Test the normalize_column_name function
test_cases = [
    'Display Name',
    'Sales %',
    'Q&A Section',
    'Item #',
    '2024 Revenue',
    'customer_id',
    'First & Last Name',
    'Price (USD)',
    '___messy___name___',
    '',
    'Année',  # Non-ASCII
]

print("Column Name Normalization Tests:")
print("-" * 60)
for test in test_cases:
    normalized = normalize_column_name(test)
    print(f"{test!r:30s} -> {normalized!r}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Test duplicate handling
print("\n\nDuplicate Handling Test:")
print("-" * 60)

import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Create test DataFrame with duplicate normalized names
test_data = pd.DataFrame({
    'Display Name': [1, 2],
    'Display-Name': [3, 4],
    'Display_Name': [5, 6],
    'Sales %': [100, 200],
    '2024 Revenue': [1000, 2000]
})

print("Original columns:", test_data.columns.tolist())

# Normalize
test_normalized = normalize_dataframe_columns(test_data)
print("Normalized columns:", test_normalized.columns.tolist())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
