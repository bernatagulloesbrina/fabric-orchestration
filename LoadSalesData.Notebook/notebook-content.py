# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Load Sales Data
# 
# Generates dummy sales data and loads it to the **DW** warehouse.
# 
# **Parameters:**
# - `rows_to_generate` - Number of sales rows to create (default: 100)

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, expr, lit
from datetime import datetime, timedelta
import random

# PARAMETERS CELL ********************

# Number of sales rows to generate
rows_to_generate = 100

# MARKDOWN ********************

# ## Generate Dummy Sales Data

# CELL ********************

spark = SparkSession.builder.getOrCreate()

# Generate date range (last 30 days)
base_date = datetime.now() - timedelta(days=30)

# Create dummy sales data
sales_data = []
for i in range(rows_to_generate):
    days_offset = random.randint(0, 30)
    sale_date = base_date + timedelta(days=days_offset)
    amount = round(random.uniform(10.0, 1000.0), 2)
    
    sales_data.append({
        'sale_date': sale_date.strftime('%Y-%m-%d'),
        'amount': amount
    })

# Create DataFrame
df_sales = spark.createDataFrame(sales_data)

# Add sale_id (will use NEWID() in warehouse) and created_at timestamp
df_sales = df_sales.withColumn('sale_id', expr('uuid()')) \
                   .withColumn('created_at', current_timestamp())

print(f'Generated {df_sales.count()} sales records')
df_sales.show(10)

# MARKDOWN ********************

# ## Load to DW Warehouse
# 
# Writes the sales data to the **Sales** table in DW warehouse.
# Data is appended (not overwritten).

# CELL ********************

# Write to DW warehouse - use 3-part naming: warehouse.schema.table
# Note: DW warehouse must be attached to this notebook or use full connection

try:
    # Option 1: If DW warehouse is attached as default
    df_sales.write.format("delta") \
        .mode("append") \
        .saveAsTable("dw.dbo.sales")
    
    print(f'✓ Loaded {rows_to_generate} sales records to DW.dbo.Sales')
    
except Exception as e:
    # Option 2: Write to lakehouse first, then use SQL to load to warehouse
    print(f'Note: Direct warehouse write failed - {str(e)}')
    print(f'Attempting lakehouse staging approach...')
    
    # Write to temp table in default lakehouse
    df_sales.write.format("delta").mode("overwrite").saveAsTable("sales_staging")
    
    print(f'✓ Staged {rows_to_generate} sales records')
    print(f'⚠️  Please use SQL to load from staging to DW:')
    print(f'   INSERT INTO DW.dbo.Sales SELECT * FROM sales_staging')

# CELL ********************

print('=' * 60)
print('✓ Sales data load complete!')
print('=' * 60)
