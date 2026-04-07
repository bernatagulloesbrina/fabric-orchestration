# Setup Guide

Complete setup instructions for the Fabric orchestration system.

## 1. Create Fabric Artifacts

Create these artifacts in your Fabric workspace:

- **MetadataLakehouse** - Stores tenant metadata
- **StagingLakehouse** - Staging area for data loading
- **Metadata** (SQL Database) - Control tables for job tracking
- **DW** (Warehouse) - Analytics warehouse (optional)
- **Environment** - For library dependencies and configuration

## 2. Configure Spark Properties

To avoid reconfiguring notebooks after every Git sync, set Spark properties in your **Environment** artifact:

### Steps:

1. Open your **Environment** artifact in Fabric
2. Go to **Spark properties** tab (under Spark compute section)
3. Add this custom property:
   - **Key**: `spark.fabric.metadata.sql.server`
   - **Value**: `your-workspace.datawarehouse.fabric.microsoft.com`
     - Get from: Metadata SQL Database > Settings > SQL connection string
     - Example: `abc123xyz.datawarehouse.fabric.microsoft.com`

4. Click **Publish** to save the Environment
5. Attach this Environment to **LogPipelineExecution** notebook

### Why Spark Properties?

✅ **Survives Git sync** - your configuration won't be overwritten
✅ **Centralized** - manage all notebooks from one Environment
✅ **Native to Fabric** - uses built-in Spark configuration system
✅ **No hardcoding** - keeps server name out of notebook code

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

### "Unauthorized" Error When Running Pipeline
If you get authentication errors when running LogPipelineExecution from a pipeline:

**Root Cause:** The pipeline notebook activity isn't configured with proper workspace permissions.

**Solution:**
1. **Check Pipeline Activity Settings:**
   - Open the pipeline in Fabric
   - Click on the LogPipelineExecution notebook activity
   - Go to **Settings** tab
   - Ensure **"Run with workspace identity"** is enabled/selected
   
2. **Verify Workspace Permissions:**
   - The workspace must have access to the SQL Database
   - Check workspace roles and permissions
   
3. **Alternative: Test Notebook Directly First**
   - Run the LogPipelineExecution notebook directly (not from pipeline) with test parameters
   - If it works directly but fails in pipeline → it's a pipeline activity configuration issue
   - If it fails directly → it's a workspace permission issue

### Spark Property Not Found
- Ensure Environment is attached to the notebook
- Check property key is exactly: `spark.fabric.metadata.sql.server`
- Verify Environment is published (not draft)
- Restart the Spark session if needed

### SQL Database Connection Failed
- Verify server format (no `https://`, no `/database`, no `:1433`)
- Example: `abc123xyz.datawarehouse.fabric.microsoft.com`
- Check Metadata SQL Database is in the same workspace
- Ensure notebook has workspace permissions

### Lakehouse Not Found
- Verify lakehouse name matches exactly
- Check lakehouse is attached to notebook in Fabric UI
- Ensure lakehouse exists in the workspace
