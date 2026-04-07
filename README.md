# fabric-orchestration

Metadata-driven orchestration system for Microsoft Fabric with execution tracking and conditional processing.

## Architecture

### Fabric Artifacts

```
MetadataLakehouse       - Stores tenant metadata (workspaces, models, dataflows, pipelines)
StagingLakehouse        - Staging area for data loading (e.g., sales_staging table)
Metadata (SQL Database) - Transactional control tables (jobs, executions)
DW (Warehouse)          - Analytics warehouse (sales table)
```

### Why Two Lakehouses?

- **MetadataLakehouse**: Dedicated to Fabric tenant metadata harvested by LoadFabricItems
- **StagingLakehouse**: Dedicated to transient data staging for warehouse loading
- This separation provides cleaner architecture and independent lifecycle management

### Notebooks

- **LoadFabricItems**: Harvests Fabric artifacts via REST API → writes to MetadataLakehouse
- **LoadSalesData**: Generates dummy sales → stages in StagingLakehouse
- **LogPipelineExecution**: Logs execution tracking to Metadata SQL Database

### Pipelines

- **Load Fabric Items**: Runs LoadFabricItems notebook
- **Load Sales**: Orchestrates sales loading with execution tracking (Log Start → Load → Log Success/Failure)

## Workspace items


