# fabric-orchestration

Conditionally refresh Fabric artifacts based on execution log results.

## Workspace items

### Databases / Warehouses

| Item | Type | Purpose |
|------|------|---------|
| `Metadata` | Fabric SQL Database | Stores execution logs and artifact metadata |
| `DW` | Fabric Warehouse | Sales data destination |

### Tables

#### DW (Warehouse)

| Table | Description |
|-------|-------------|
| `dbo.Sales` | Dummy sales rows (SaleDate, Amount, CreatedAt) |

#### Metadata (SQL Database)

| Table | Description |
|-------|-------------|
| `dbo.refresh_logs` | Pipeline execution log (pipeline_name, start_time, end_time, result) |
| `dbo.workspaces` | Fabric workspaces harvested by the Load Artifacts pipeline |
| `dbo.dataflows` | Dataflows per workspace |
| `dbo.semantic_models` | Semantic models per workspace |
| `dbo.pipelines` | Data pipelines per workspace |

### Pipelines

#### Load Sales

Simulates an ETL that loads sales data.

1. Records the pipeline start timestamp in a variable.
2. Inserts 10 dummy sales rows (current date + random amount) into `DW.dbo.Sales`.
3. On **success**: writes a `Success` record to `Metadata.dbo.refresh_logs` with pipeline name, start time, end time.
4. On **failure**: writes a `Failed` record to `Metadata.dbo.refresh_logs`.

#### Load Artifacts

Uses the [Fabric REST API](https://learn.microsoft.com/en-us/rest/api/fabric/articles/) to snapshot all workspace artifacts into the Metadata database.

1. Truncates the four artifact tables to ensure a fresh full snapshot.
2. Calls `GET /v1/workspaces` and bulk-inserts results into `dbo.workspaces`.
3. Iterates over every workspace (parallel ForEach, batch size 10) and for each:
   - Calls `GET /v1/workspaces/{id}/dataflows` → inserts into `dbo.dataflows`.
   - Calls `GET /v1/workspaces/{id}/semanticModels` → inserts into `dbo.semantic_models`.
   - Calls `GET /v1/workspaces/{id}/dataPipelines` → inserts into `dbo.pipelines`.

Authentication uses the **Workspace Identity** (managed identity) of the Fabric workspace.

## Repository structure

```
├── DW.Warehouse/
│   └── dbo/Tables/
│       └── Sales.sql                       # DDL for the Sales table
├── Metadata.SQLDatabase/
│   └── dbo/Tables/
│       ├── refresh_logs.sql
│       ├── workspaces.sql
│       ├── dataflows.sql
│       ├── semantic_models.sql
│       └── pipelines.sql
├── Load Sales.DataPipeline/
│   ├── pipeline-content.json               # Pipeline definition
│   ├── item.metadata.json
│   └── item.config.json
└── Load Artifacts.DataPipeline/
    ├── pipeline-content.json               # Pipeline definition
    ├── item.metadata.json
    └── item.config.json
```

## Connections

The pipelines reference two connections by logical ID.  
Update `item.config.json` in each pipeline folder with the actual connection logical IDs from your Fabric workspace.

| `connectionName` | Target | Logical ID placeholder |
|-----------------|--------|------------------------|
| `DWConnection` | Fabric Warehouse – DW | `c0000000-0000-0000-0000-000000000001` |
| `MetadataConnection` | Fabric SQL Database – Metadata | `c0000000-0000-0000-0000-000000000002` |
