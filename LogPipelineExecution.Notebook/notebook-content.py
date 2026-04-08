# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "c2e16b6d-5a9e-ae09-42cf-d0385ba8ec5f",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Log Pipeline Execution
# 
# Logs pipeline execution status to the Metadata SQL Database.
# Handles both **start** and **end** logging with MERGE to jobs table.
# 
# **Parameters:**
# - `execution_id` - Pipeline run ID (from pipeline().RunId)
# - `job_name` - Logical job name
# - `action` - 'start' | 'success' | 'failure'
# - `error_message` - Error details (only for failure)
# 
# **Configuration (one-time setup):**
# - Set Spark property in your Environment artifact (Spark properties section)
# - This persists across notebook updates from Git

# CELL ********************

import pyodbc
import struct
import notebookutils
from pyspark.sql import SparkSession
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

execution_id = 'a'
job_name = 'b'
action = 'start'  # 'start' | 'success' | 'failure'
error_message = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration
# 
# **Recommended Setup (survives notebook updates):**
# 
# In your Environment artifact > Spark properties tab, add TWO properties:
# 1. **spark.fabric.metadata.sql.server**
#    - Value: Get from SQL Database > Settings > Connection strings > Data Source
#    - Example: `abc123.database.fabric.microsoft.com`
# 
# 2. **spark.fabric.metadata.sql.database**
#    - Value: Get from SQL Database > Settings > Connection strings > Initial Catalog  
#    - Example: `Metadata-bffdcec2-818c-4e08-ba43-281a18f11b07`
#    - ⚠️ Include the full name with GUID suffix!

# CELL ********************

# Get Spark session and read configuration
spark = SparkSession.builder.getOrCreate()

# Read server from Spark property (survives Git sync) or use fallback
sql_database_server = spark.conf.get(
    'spark.fabric.metadata.sql.server', 
    'your-workspace.database.fabric.microsoft.com'  # fallback only
)

# Read database name from Spark property (survives Git sync) or use fallback
sql_database_name = spark.conf.get(
    'spark.fabric.metadata.sql.database',
    'Metadata'  # fallback - update if your database has GUID suffix
)

print(f'Using SQL Database server: {sql_database_server}')
print(f'Using SQL Database name: {sql_database_name}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate parameters
if not execution_id or not job_name:
    raise ValueError('execution_id and job_name are required parameters')

if action not in ['start', 'success', 'failure']:
    raise ValueError('action must be: start, success, or failure')

print(f'Logging {action} for job: {job_name}')
print(f'Execution ID: {execution_id}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Connect to SQL Database

# CELL ********************

# Get session token for authentication
# Note: This requires the pipeline/notebook to run with workspace identity permissions
try:
    token = notebookutils.credentials.getToken('https://database.windows.net/')
    print('✓ Successfully obtained authentication token')
except Exception as e:
    print(f'❌ Failed to get authentication token: {str(e)}')
    print('\nTroubleshooting:')
    print('1. Ensure the notebook is attached to the workspace (not running in isolation)')
    print('2. Check that the workspace has SQL Database permissions')
    print('3. Verify the pipeline activity is using workspace identity')
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Debug: Decode token to see identity information
import base64
import json

# JWT tokens have 3 parts separated by dots: header.payload.signature
token_parts = token.split('.')
if len(token_parts) >= 2:
    # Decode the payload (2nd part)
    # Add padding if needed for base64 decoding
    payload = token_parts[1]
    payload += '=' * (4 - len(payload) % 4)
    
    try:
        decoded_bytes = base64.urlsafe_b64decode(payload)
        decoded_json = json.loads(decoded_bytes)
        
        print('🔍 Token Identity Information:')
        print(f"  Audience (aud): {decoded_json.get('aud', 'N/A')}")
        print(f"  Issuer (iss): {decoded_json.get('iss', 'N/A')}")
        
        # Check what type of identity
        if 'upn' in decoded_json:
            print(f"  ✓ User Principal Name (upn): {decoded_json['upn']}")
            print(f"  → This is a USER identity token")
        elif 'oid' in decoded_json:
            print(f"  ✓ Object ID (oid): {decoded_json['oid']}")
            if 'appid' in decoded_json:
                print(f"  ✓ Application ID (appid): {decoded_json['appid']}")
                print(f"  → This is a SERVICE PRINCIPAL/MANAGED IDENTITY token")
            else:
                print(f"  → This is a workspace/managed identity token")
        
        print(f"\n💡 This identity needs permissions on the SQL Database artifact in Fabric")
        
    except Exception as decode_error:
        print(f'⚠️  Could not decode token: {decode_error}')
else:
    print('⚠️  Token format unexpected')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token_bytes = token.encode('utf-16-le')
token_struct = struct.pack('=I', len(token_bytes)) + token_bytes

# Build connection string
conn_str = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={sql_database_server};'
    f'DATABASE={sql_database_name};'
    'Encrypt=yes;TrustServerCertificate=no;'
)

print(f'\n🔌 Connecting to: {sql_database_server}/{sql_database_name}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Connect with token authentication
try:
    connection = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    connection.autocommit = True
    cursor = connection.cursor()
    print(f'✓ Connected to Metadata SQL Database')
    
except pyodbc.Error as db_error:
    print(f'❌ Failed to connect to SQL Database')
    print(f'Error: {str(db_error)}')
    print('\n🔧 Troubleshooting Steps:')
    print('\n1. Grant Permissions on SQL Database:')
    print('   - Open "Metadata" SQL Database in Fabric portal')
    print('   - Go to Settings → Manage permissions')
    print('   - Add the identity shown above (user or workspace)')
    print('   - Grant "Contributor" role')
    print('\n2. Verify SQL Database Server Name:')
    print(f'   - Current: {sql_database_server}')
    print('   - Expected format: [workspace-id].datawarehouse.fabric.microsoft.com')
    print('   - Get correct value from: SQL Database → Settings → Connection strings')
    print('\n3. Ensure Same Workspace:')
    print('   - SQL Database and this notebook must be in the same workspace')
    print('   - Cross-workspace requires explicit sharing')
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Execute Logging SQL

# CELL ********************

current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

if action == 'start':
    # Insert execution start
    sql = f"""
    INSERT INTO dbo.executions (execution_id, job_name, start_time, created_at)
    VALUES ('{execution_id}', '{job_name}', '{current_time}', '{current_time}')
    """
    cursor.execute(sql)
    
    # MERGE to jobs table - record start time
    sql_merge = f"""
    MERGE dbo.jobs AS target
    USING (SELECT 
        '{job_name}' AS job_name,
        '{current_time}' AS last_start_time,
        '{current_time}' AS updated_at
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            last_start_time = source.last_start_time,
            updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (job_name, last_start_time, updated_at)
        VALUES (source.job_name, source.last_start_time, source.updated_at);
    """
    
    try:
        cursor.execute(sql_merge)
        rows_affected = cursor.rowcount
        print(f'✓ Logged execution start (executions table)')
        print(f'✓ Updated jobs table ({rows_affected} row affected)')
    except Exception as merge_error:
        print(f'⚠️  Warning: Failed to update jobs table: {str(merge_error)}')
        print(f'   Execution was logged in executions table successfully')
        # Don't raise - execution logging succeeded

elif action == 'success':
    # Update execution end
    sql_update = f"""
    UPDATE dbo.executions
    SET end_time = '{current_time}', result = 'Success'
    WHERE execution_id = '{execution_id}'
    """
    cursor.execute(sql_update)
    
    # MERGE to jobs table
    sql_merge = f"""
    MERGE dbo.jobs AS target
    USING (SELECT 
        '{job_name}' AS job_name,
        '{current_time}' AS last_end_time,
        'Success' AS last_result,
        NULL AS error_message,
        '{current_time}' AS updated_at
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            last_end_time = source.last_end_time,
            last_result = source.last_result,
            error_message = source.error_message,
            updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (job_name, last_end_time, last_result, error_message, updated_at)
        VALUES (source.job_name, source.last_end_time, source.last_result, source.error_message, source.updated_at);
    """
    
    try:
        cursor.execute(sql_merge)
        rows_affected = cursor.rowcount
        print(f'✓ Logged success (executions table)')
        print(f'✓ Updated jobs table ({rows_affected} row affected)')
    except Exception as merge_error:
        print(f'⚠️  Warning: Failed to update jobs table: {str(merge_error)}')
        print(f'   Execution was logged in executions table successfully')
        # Don't raise - execution logging succeeded

elif action == 'failure':
    # Escape single quotes in error message
    error_msg_escaped = error_message.replace("'", "''") if error_message else ''
    
    # Update execution end with error
    sql_update = f"""
    UPDATE dbo.executions
    SET end_time = '{current_time}', result = 'Failed', error_message = '{error_msg_escaped}'
    WHERE execution_id = '{execution_id}'
    """
    cursor.execute(sql_update)
    
    # MERGE to jobs table
    sql_merge = f"""
    MERGE dbo.jobs AS target
    USING (SELECT 
        '{job_name}' AS job_name,
        '{current_time}' AS last_end_time,
        'Failed' AS last_result,
        '{error_msg_escaped}' AS error_message,
        '{current_time}' AS updated_at
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            last_end_time = source.last_end_time,
            last_result = source.last_result,
            error_message = source.error_message,
            updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (job_name, last_end_time, last_result, error_message, updated_at)
        VALUES (source.job_name, source.last_end_time, source.last_result, source.error_message, source.updated_at);
    """
    
    try:
        cursor.execute(sql_merge)
        rows_affected = cursor.rowcount
        print(f'✓ Logged failure (executions table)')
        print(f'✓ Updated jobs table ({rows_affected} row affected)')
    except Exception as merge_error:
        print(f'⚠️  Warning: Failed to update jobs table: {str(merge_error)}')
        print(f'   Execution was logged in executions table successfully')
        # Don't raise - execution logging succeeded

connection.close()
print(f'✓ Execution logging complete')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
