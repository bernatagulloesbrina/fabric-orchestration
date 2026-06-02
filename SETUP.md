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
3. Add these custom properties:
   
   **Property 1:**
   - **Key**: `spark.fabric.metadata.sql.server`
   - **Value**: Get from Metadata SQL Database > Settings > Connection strings > **Data Source**
   - Example: `abc123xyz.database.fabric.microsoft.com`
   - ⚠️ Do NOT include port `,1433` or `https://`
   
   **Property 2:**
   - **Key**: `spark.fabric.metadata.sql.database`
   - **Value**: Get from Metadata SQL Database > Settings > Connection strings > **Initial Catalog**
   - Example: `Metadata-bffdcec2-818c-4e08-ba43-281a18f11b07`
   - ⚠️ Include the full name with GUID suffix!

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
-- 4. scripts/metadata/04_create_logging_procedures.sql
-- 5. scripts/metadata/05_create_udf_config.sql  (see step 6 before running)
-- 6. scripts/metadata/06_create_views.sql
```

Script 6 creates `dbo.vw_jobs` and `dbo.vw_executions` — identical to their base tables but with all timestamps converted from UTC to CET/CEST. DST is applied automatically per row using `AT TIME ZONE 'Central European Standard Time'` (UTC+1 in winter, UTC+2 in summer).

## 6. Configure On-Demand Refresh UDF

The `triggerOnDemandRefresh` User Data Function calls the Fabric REST API using a service principal. The credentials are stored in the metadata database (not in code or the repository).

### Steps:

1. **Create an app registration in Entra ID**
   - Azure Portal → Entra ID → App registrations → New registration
   - Name: `fabric-orchestration-udf` (or similar)
   - Single tenant, no redirect URI needed
   - Note the **Directory (tenant) ID** and **Application (client) ID** from the Overview page
   - Go to **Certificates & secrets → New client secret**, set an expiry, and copy the **Value** immediately

2. **Grant workspace access to the service principal**
   - Go to your Fabric workspace → **Manage access → Add people or groups**
   - Search for the app registration name
   - Assign **Contributor** role

3. **Store the credentials in the metadata database**
   - Open `scripts/metadata/05_create_udf_config.sql`
   - Replace `<your-tenant-id>`, `<your-client-id>`, and `<your-client-secret>` with the real values
   - Run the script in the **Metadata** SQL Database query editor
   - ⚠️ Do not commit the file after filling in real values — discard the local change

   > **`PIPELINE_WORKSPACE_NAME`** is the display name of the Fabric workspace where pipeline `09_Refresh Fabric Item On Demand` lives — not the pipeline name itself. The Fabric REST API has no cross-workspace search, so the UDF must first resolve the workspace ID before it can look up the pipeline by name. Use the exact name shown in the top-left of the Fabric portal (e.g. `My Fabric Workspace`).

4. **Create the UDF in Fabric**
   - In your Fabric workspace, create a new **User Data Function** item named `triggerOnDemandRefresh`
   - Connect it to the **Metadata** SQL Database (alias: `Metadata`)
   - Add `azure-identity` and `requests` to the UDF libraries
   - Paste the contents of `Refresh Config/triggerOnDemandRefresh.UserDataFunction/function_app.py` into the editor

## 6b. Enable Datasource Extraction (Metadata Scanning)

The **LoadFabricItems** notebook builds a `refresh_job_sources` table describing what each
refreshable item (semantic model / dataflow) reads from. It calls the Power BI **admin
metadata scanner** (`admin/workspaces/getInfo`) using the **identity that runs the notebook**
(`notebookutils.credentials.getToken("pbi")`) — no service principal secret is read.

Whoever/whatever runs the notebook must be allowed to call read-only admin APIs + metadata
scanning:

1. **Fabric Admin Portal → Tenant settings → Admin API settings:**
   - Enable **"Enhanced metadata scanning"** (required for `datasourceDetails=true`).
   - Enable **"Service principals can access read-only admin APIs"** and add the **workspace
     identity** (and/or your SP) to the allowed security group — needed for **pipeline
     (headless)** runs, which execute as the workspace identity.
2. **Interactive runs** use *your* signed-in identity, so they work as long as you are a
   **Fabric admin**.

> If scanning is not enabled, or the identity is not authorized, `refresh_job_sources` is
> written as an empty table (with a warning) and the rest of the items load is unaffected.
>
> **Fallback (explicit service principal):** if the workspace-identity token is rejected for
> the admin APIs in pipeline runs, store the SP secret in **Azure Key Vault**, read it in the
> notebook with `notebookutils.credentials.getSecret(<vaultUri>, <secretName>)`, and exchange
> it for a Power BI token via the client-credentials grant
> (`scope=https://analysis.windows.net/powerbi/api/.default`). Reading the SP secret directly
> from `dbo.udf_config` is **not** viable here: Fabric Spark notebooks can't obtain a SQL
> access token via `notebookutils.credentials.getToken` (no database audience).

## 7. Create Warehouse Table (Optional)

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

**Root Cause:** The workspace doesn't have permissions to access the Metadata SQL Database.

**Solution:**
1. **Grant Permissions on SQL Database:**
   - Open **Metadata** SQL Database in Fabric portal
   - Click the **"..." menu** → **Manage permissions** (or **Settings** → **Permissions**)
   - Click **+ Add user or group**
   - Add your **workspace name** or **your user account**
   - Select **Contributor** role (grants read/write access)
   
2. **Verify Same Workspace:**
   - Ensure SQL Database, notebooks, and pipeline are all in the **same workspace**
   - Cross-workspace access requires explicit sharing
   
3. **Test Notebook Directly:**
   - Open LogPipelineExecution notebook in Fabric
   - Manually set test parameters and run it
   - If it works directly but fails in pipeline → unusual (contact support)
   - If it fails directly → permissions issue confirmed

**Note:** Fabric SQL Database uses Azure AD (Entra ID) authentication only - no SQL logins.

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
