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
# and loads them into the **Metadata** SQL Database.
# 
# **Before running:** Set `SQL_DATABASE_SERVER` in the Parameters cell (get from Fabric portal).
# Authentication uses session token automatically - no password needed!

# CELL ********************

import sempy.fabric as fabric
import pyodbc
import struct  
import notebookutils
from datetime import datetime, timezone

# PARAMETERS CELL ********************

# Name of the SQL Database artifact in the current workspace  
SQL_DATABASE_NAME = 'Metadata'

# CELL ********************

def get_sql_connection_from_workspace(database_name):
    """
    Gets SQL Database connection using workspace context.
    
    SETUP (one-time):
    1. In Fabric portal, go to your Metadata SQL Database
    2. Go to Settings > Connection strings
    3. Copy the SERVER value (e.g., abc12-xyz34.database.fabric.microsoft.com)
    4. Store it as an environment variable or update this function
    
    OR add the SQL Database as a connection/data source to this notebook in Fabric UI.
    """
    # Get server from settings (one-time configuration needed)
    # TODO: Replace with your server name from connection strings
    server = 'your-server-id.database.fabric.microsoft.com'
    
    # Use session token for authentication (no password needed)
    token = notebookutils.credentials.getToken('https://database.windows.net/')
    token_bytes = token.encode('utf-16-le')
    token_struct = struct.pack('=I', len(token_bytes)) + token_bytes
    
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database_name};'
        'Encrypt=yes;TrustServerCertificate=no;'
    )
    
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    conn.autocommit = True
    return conn

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

# Helper function to escape single quotes for SQL
def escape_sql(value):
    """Escape single quotes in SQL string values."""
    return str(value).replace("'", "''")

# Truncate tables
for table in ['dbo.workspaces', 'dbo.semantic_models', 'dbo.dataflows', 'dbo.pipelines']:
    cursor.execute(f'TRUNCATE TABLE {table}')

print('Tables truncated.')

# Insert workspaces
if workspaces:
    workspaces_sql = "INSERT INTO dbo.workspaces (workspace_id, display_name, type, state, harvested_at) VALUES\n"
    workspaces_values = []
    for ws in workspaces:
        ws_id = escape_sql(ws.get('id'))
        display_name = escape_sql(ws.get('displayName', ''))
        ws_type = escape_sql(ws.get('type', ''))
        state = escape_sql(ws.get('state', ''))
        workspaces_values.append(f"('{ws_id}', '{display_name}', '{ws_type}', '{state}', '{harvested_at}')")
    workspaces_sql += ",\n".join(workspaces_values) + ";"
    cursor.execute(workspaces_sql)
    print(f'Inserted {len(workspaces)} workspaces.')

# Insert semantic models
if semantic_models:
    semantic_models_sql = "INSERT INTO dbo.semantic_models (workspace_id, item_id, display_name, description, harvested_at) VALUES\n"
    semantic_models_values = []
    for w, i, n, d in semantic_models:
        workspace_id = escape_sql(w)
        item_id = escape_sql(i)
        display_name = escape_sql(n)
        description = escape_sql(d)
        semantic_models_values.append(f"('{workspace_id}', '{item_id}', '{display_name}', '{description}', '{harvested_at}')")
    semantic_models_sql += ",\n".join(semantic_models_values) + ";"
    cursor.execute(semantic_models_sql)
    print(f'Inserted {len(semantic_models)} semantic models.')

# Insert dataflows
if dataflows:
    dataflows_sql = "INSERT INTO dbo.dataflows (workspace_id, item_id, display_name, harvested_at) VALUES\n"
    dataflows_values = []
    for w, i, n in dataflows:
        workspace_id = escape_sql(w)
        item_id = escape_sql(i)
        display_name = escape_sql(n)
        dataflows_values.append(f"('{workspace_id}', '{item_id}', '{display_name}', '{harvested_at}')")
    dataflows_sql += ",\n".join(dataflows_values) + ";"
    cursor.execute(dataflows_sql)
    print(f'Inserted {len(dataflows)} dataflows.')

# Insert pipelines
if pipelines:
    pipelines_sql = "INSERT INTO dbo.pipelines (workspace_id, item_id, display_name, harvested_at) VALUES\n"
    pipelines_values = []
    for w, i, n in pipelines:
        workspace_id = escape_sql(w)
        item_id = escape_sql(i)
        display_name = escape_sql(n)
        pipelines_values.append(f"('{workspace_id}', '{item_id}', '{display_name}', '{harvested_at}')")
    pipelines_sql += ",\n".join(pipelines_values) + ";"
    cursor.execute(pipelines_sql)
    print(f'Inserted {len(pipelines)} pipelines.')

connection.close()
print('✓ Artifact tables refreshed successfully.')
