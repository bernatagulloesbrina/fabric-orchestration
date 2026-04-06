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
# **Before running:** update `METADATA_SERVER` in the Parameters cell below.

# CELL ********************

import sempy.fabric as fabric
import pyodbc
import struct
import notebookutils
from datetime import datetime, timezone

# PARAMETERS CELL ********************

# UPDATE: replace with your Metadata SQL Database fully-qualified server name
# Found in: Fabric portal -> Metadata SQL Database -> Settings -> Connection strings
METADATA_SERVER   = 'your-server.database.fabric.microsoft.com'
METADATA_DATABASE = 'Metadata'

# CELL ********************

def get_sql_connection(server, database):
    """Opens a pyodbc connection to a Fabric SQL Database using the session identity token."""
    token        = notebookutils.credentials.getToken('https://database.windows.net/')
    token_bytes  = token.encode('utf-16-le')
    token_struct = struct.pack('=I', len(token_bytes)) + token_bytes
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        'Encrypt=yes;TrustServerCertificate=no;'
    )
    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    conn.autocommit = True
    return conn


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

    for item in fetch_all(client, f'/v1/workspaces/{ws_id}/items?type=DataflowsGen2'):
        dataflows.append((ws_id, item['id'], item.get('displayName', '')))

    for item in fetch_all(client, f'/v1/workspaces/{ws_id}/items?type=DataPipeline'):
        pipelines.append((ws_id, item['id'], item.get('displayName', '')))

print(f'Semantic models : {len(semantic_models)}')
print(f'Dataflows (Gen2): {len(dataflows)}')
print(f'Pipelines       : {len(pipelines)}')

# CELL ********************

conn   = get_sql_connection(METADATA_SERVER, METADATA_DATABASE)
cursor = conn.cursor()

for tbl in ['dbo.workspaces', 'dbo.semantic_models', 'dbo.dataflows', 'dbo.pipelines']:
    cursor.execute(f'TRUNCATE TABLE {tbl}')

cursor.executemany(
    'INSERT INTO dbo.workspaces (workspace_id, display_name, type, state, harvested_at) VALUES (?,?,?,?,?)',
    [(ws.get('id'), ws.get('displayName', ''), ws.get('type', ''), ws.get('state', ''), harvested_at)
     for ws in workspaces]
)

cursor.executemany(
    'INSERT INTO dbo.semantic_models (workspace_id, item_id, display_name, description, harvested_at) VALUES (?,?,?,?,?)',
    [(w, i, n, d, harvested_at) for w, i, n, d in semantic_models]
)

cursor.executemany(
    'INSERT INTO dbo.dataflows (workspace_id, item_id, display_name, harvested_at) VALUES (?,?,?,?)',
    [(w, i, n, harvested_at) for w, i, n in dataflows]
)

cursor.executemany(
    'INSERT INTO dbo.pipelines (workspace_id, item_id, display_name, harvested_at) VALUES (?,?,?,?)',
    [(w, i, n, harvested_at) for w, i, n in pipelines]
)

conn.close()
print('Artifact tables refreshed successfully.')
