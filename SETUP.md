# Setup Guide

Complete setup instructions for the Fabric orchestration system.

## 1. Create Fabric Artifacts

Create these artifacts in your Fabric workspace:

- **MetadataLakehouse** - Stores tenant metadata
- **StagingLakehouse** - Staging area for data loading
- **Metadata** (SQL Database) - Control tables for job tracking
- **DW** (Warehouse) - Analytics warehouse (optional)
- **Environment** - For library dependencies and configuration

## 2. Configure Environment Variables

To avoid reconfiguring notebooks after every Git sync, set environment variables in your **Environment** artifact:

### Steps:

1. Open your **Environment** artifact in Fabric
2. Go to **Settings** > **Environment variables**
3. Add this variable:
   - **Name**: `SQL_DATABASE_SERVER`
   - **Value**: `your-workspace.datawarehouse.fabric.microsoft.com`
     - Get from: Metadata SQL Database > Settings > SQL connection string
     - Example: `abc123xyz.datawarehouse.fabric.microsoft.com`

4. Save the Environment
5. Attach this Environment to **LogPipelineExecution** notebook

### Why Environment Variables?

✅ **Survives Git sync** - your configuration won't be overwritten
✅ **Centralized** - manage all notebooks from one place
✅ **Secure** - no hardcoded values in notebooks

## 3. Sync from Git

Sync your workspace from Git to pull all notebooks and pipelines.

## 4. Attach Lakehouses and Environment

In Fabric, attach resources to notebooks:

### Lakehouse Attachments:
- **LoadFabricItems** → attach **MetadataLakehouse**
- **LoadSalesData** → attach **StagingLakehouse**
- **LogPipelineExecution** → no lakehouse needed

### Environment Attachment:
- **LogPipelineExecution** → attach your **Environment** (for SQL_DATABASE_SERVER variable)

## 5. Create SQL Database Tables

Run these scripts in **Metadata** SQL Database query editor:

```sql
-- Run in order:
-- 1. scripts/metadata/01_create_jobs.sql
-- 2. scripts/metadata/02_create_executions.sql
-- 3. scripts/metadata/03_create_artifact_tables.sql
```

## 6. Create Warehouse Table (Optional)

If using DW warehouse, run in **DW** query editor:

```sql
-- scripts/dw/01_create_sales.sql
```

## 7. Build Pipelines in Fabric UI

Since pipelines are synced as empty placeholders, configure activities in Fabric:

### Load Fabric Items Pipeline:
1. **Run Notebook** activity
   - Notebook: LoadFabricItems

### Load Sales Pipeline:
1. **Log Start** (Notebook activity)
   - Notebook: LogPipelineExecution
   - Parameters:
     - `execution_id`: `@pipeline().RunId`
     - `job_name`: `Load Sales`
     - `action`: `start`

2. **Load Sales Data** (Notebook activity)
   - Notebook: LoadSalesData
   - On success only → continue

3. **Log Success** (Notebook activity, on success)
   - Notebook: LogPipelineExecution
   - Parameters:
     - `execution_id`: `@pipeline().RunId`
     - `job_name`: `Load Sales`
     - `action`: `success`

4. **Log Failure** (Notebook activity, on failure path)
   - Notebook: LogPipelineExecution
   - Parameters:
     - `execution_id`: `@pipeline().RunId`
     - `job_name`: `Load Sales`
     - `action`: `failure`
     - `error_message`: `@activity('Load Sales Data').error.message`

## 8. Test

1. Run **Load Fabric Items** pipeline
   - Verify metadata tables populated in MetadataLakehouse
   
2. Run **Load Sales** pipeline
   - Verify execution logged in Metadata SQL Database
   - Check `dbo.jobs` and `dbo.executions` tables

## Troubleshooting

### Environment Variable Not Found
- Ensure Environment is attached to the notebook
- Check variable name is exactly: `SQL_DATABASE_SERVER`
- Verify Environment is published (not draft)

### SQL Database Connection Failed
- Verify SQL_DATABASE_SERVER format (no `https://`, no `/database`)
- Check Metadata SQL Database is in the same workspace
- Ensure notebook has workspace permissions

### Lakehouse Not Found
- Verify lakehouse name matches exactly
- Check lakehouse is attached to notebook in Fabric UI
- Ensure lakehouse exists in the workspace
