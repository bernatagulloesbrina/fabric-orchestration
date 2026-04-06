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
# and loads them into the **Metadata** SQL Database (same workspace).
# 
# Uses `%%tsql` magic command - no connection string needed!

# CELL ********************

import sempy.fabric as fabric
import notebookutils
from datetime import datetime, timezone

# PARAMETERS CELL ********************

# Name of the SQL Database artifact in the current workspace
SQL_DATABASE_NAME = 'Metadata'

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
# Connects to the SQL Database artifact by name (no hardcoded server/connection string needed!)

# CELL ********************

# Connect to SQL Database artifact in the current workspace
connection = notebookutils.data.connect_to_artifact(SQL_DATABASE_NAME, artifact_type="SQLDatabase")

# Helper function to escape single quotes for SQL
def escape_sql(value):
    """Escape single quotes in SQL string values."""
    return str(value).replace("'", "''")

# Truncate tables
for table in ['dbo.workspaces', 'dbo.semantic_models', 'dbo.dataflows', 'dbo.pipelines']:
    connection.execute(f'TRUNCATE TABLE {table}')

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
    connection.execute(workspaces_sql)
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
    connection.execute(semantic_models_sql)
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
    connection.execute(dataflows_sql)
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
    connection.execute(pipelines_sql)
    print(f'Inserted {len(pipelines)} pipelines.')

print('✓ Artifact tables refreshed successfully.')
