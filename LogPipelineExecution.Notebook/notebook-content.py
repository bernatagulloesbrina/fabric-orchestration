# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
# **One-Time Setup:**
# - Update `sql_database_server` default value below (only once)
# - Or set as notebook parameter in Fabric UI settings

# CELL ********************

import pyodbc
import struct
import notebookutils
from datetime import datetime

# PARAMETERS CELL ********************

execution_id = ''
job_name = ''
action = 'start'  # 'start' | 'success' | 'failure'
error_message = ''

# ═══════════════════════════════════════════════════════════════
# ONE-TIME CONFIGURATION: Update this default value once
# Get from: Fabric portal > Metadata SQL Database > Settings > SQL connection string
# Format example: abc123xyz.datawarehouse.fabric.microsoft.com
# ═══════════════════════════════════════════════════════════════
sql_database_server = 'your-workspace.datawarehouse.fabric.microsoft.com'

# CELL ********************

# Validate parameters
if not execution_id or not job_name:
    raise ValueError('execution_id and job_name are required parameters')

if action not in ['start', 'success', 'failure']:
    raise ValueError('action must be: start, success, or failure')

print(f'Logging {action} for job: {job_name}')
print(f'Execution ID: {execution_id}')

# MARKDOWN ********************

# ## Connect to SQL Database

# CELL ********************

# Get session token for authentication
token = notebookutils.credentials.getToken('https://database.windows.net/')
token_bytes = token.encode('utf-16-le')
token_struct = struct.pack('=I', len(token_bytes)) + token_bytes

# Build connection string
conn_str = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={sql_database_server};'
    f'DATABASE=Metadata;'
    'Encrypt=yes;TrustServerCertificate=no;'
)

# Connect with token authentication
connection = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
connection.autocommit = True
cursor = connection.cursor()

print(f'✓ Connected to Metadata SQL Database')

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
    print(f'✓ Logged execution start')

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
    cursor.execute(sql_merge)
    print(f'✓ Logged success and updated jobs table')

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
    cursor.execute(sql_merge)
    print(f'✓ Logged failure and updated jobs table')

connection.close()
print(f'✓ Execution logging complete')
