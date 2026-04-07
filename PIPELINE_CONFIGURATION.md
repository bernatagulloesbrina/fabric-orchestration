# Pipeline Configuration Guide

Step-by-step instructions for building the Load Sales pipeline with proper authentication settings.

## Common Issue: "Unauthorized" Error

If you're getting authentication errors when LogPipelineExecution runs from the pipeline:

```
The caller is not authenticated to access this resource
```

**Root Cause:** The workspace (or your user account) doesn't have permissions to access the **Metadata SQL Database**.

**Solution:**

1. **Grant Workspace Permissions on SQL Database:**
   - Open **Metadata** SQL Database in Fabric
   - Click **Settings** → **Permissions** (or **Manage access**)
   - Add your workspace or user with at least `Contributor` role
   - Or grant SQL permissions: `db_datareader` + `db_datawriter`

2. **Verify Same Workspace:**
   - Ensure the SQL Database is in the **same Fabric workspace** as your pipeline
   - Cross-workspace access requires explicit sharing

3. **Check Workspace Identity:**
   - The notebook uses `notebookutils.credentials.getToken()` which requires workspace-level permissions
   - No pipeline setting needed - this is automatic in Fabric

**Test Before Pipeline:**
- Run the LogPipelineExecution notebook **directly** (not from pipeline) first
- If it fails directly → permissions issue
- If it works directly but fails in pipeline → contact Fabric support (unusual scenario)

## Load Sales Pipeline - Complete Configuration

### Activity 1: Log Start

**Activity Type:** Notebook

**Settings Tab:**
- **Notebook**: LogPipelineExecution
- **Workspace**: (should auto-select your workspace)
- **Connection**: (leave as default - notebook will use workspace credentials)

**Base Parameters:**
- `execution_id`: `@pipeline().RunId`
- `job_name`: `Load Sales` (literal string)
- `action`: `start` (literal string)
- `error_message`: (leave empty)

**On Success:** → Connect to "Load Sales Data" activity

---

### Activity 2: Load Sales Data

**Activity Type:** Notebook

**Settings Tab:**
- **Notebook**: LoadSalesData
- **Workspace**: (should auto-select your workspace)

**Base Parameters:**
- `rows_to_generate`: `100` (or your preferred value)

**On Success:** → Connect to "Log Success" activity
**On Failure:** → Connect to "Log Failure" activity

---

### Activity 3: Log Success

**Activity Type:** Notebook

**Settings Tab:**
- **Notebook**: LogPipelineExecution
- **Workspace**: (should auto-select your workspace)

**Base Parameters:**
- `execution_id`: `@pipeline().RunId`
- `job_name`: `Load Sales` (literal string)
- `action`: `success` (literal string)
- `error_message`: (leave empty)

**Dependency:**
- Runs only when "Load Sales Data" **succeeds**

---

### Activity 4: Log Failure

**Activity Type:** Notebook

**Settings Tab:**
- **Notebook**: LogPipelineExecution
- **Workspace**: (should auto-select your workspace)

**Base Parameters:**
- `execution_id`: `@pipeline().RunId`
- `job_name`: `Load Sales` (literal string)
- `action`: `failure` (literal string)
- `error_message`: `@activity('Load Sales Data').error.message`

**Dependency:**
- Runs only when "Load Sales Data" **fails**

---

## Pipeline Flow Diagram

```
┌─────────────┐
│  Log Start  │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│ Load Sales Data │
└────┬────────┬───┘
     │        │
  Success  Failure
     │        │
     ▼        ▼
┌────────┐ ┌────────┐
│Log     │ │Log     │
│Success │ │Failure │
└────────┘ └────────┘
```

---

## Testing the Pipeline

### Step 1: Test LogPipelineExecution Notebook Directly

Before running from pipeline, test the notebook directly:

1. Open LogPipelineExecution notebook in Fabric
2. Set parameter values manually:
   ```python
   execution_id = 'test-123'
   job_name = 'Test Job'
   action = 'start'
   error_message = ''
   ```
3. Run the notebook
4. Check if it connects to SQL Database and logs successfully

**If this works:** The notebook code is fine, issue is pipeline configuration
**If this fails:** Check workspace permissions and SQL server configuration

### Step 2: Test Pipeline with Minimal Configuration

1. Create a simple pipeline with just "Log Start" activity
2. Run it
3. Check the notebook run output for errors

### Step 3: Gradually Add Activities

Once "Log Start" works:
1. Add "Load Sales Data" → test
2. Add "Log Success" → test
3. Add "Log Failure" → test with intentional error

---

## Workspace Permissions Checklist

Ensure your workspace has:

- ✅ Access to the **Metadata** SQL Database (in same workspace or shared)
- ✅ **Contributor** role or higher on the workspace
- ✅ Notebooks are **attached to lakehouses** as specified in SETUP.md
- ✅ **Environment** artifact (if using Spark properties) is published and attached

---

## Quick Fix for Authentication Error

**If you see:** `Unauthorized` or `The caller is not authenticated`

**Fix:**

1. **Grant SQL Database Permissions:**
   - Open **Metadata** SQL Database in Fabric portal
   - Go to **Settings** → **Permissions** (or **Manage** → **Permissions**)
   - Click **+ Add people or groups**
   - Add your **workspace** or your **user account**
   - Grant **Contributor** role (or minimum: db_datareader + db_datawriter)

2. **Verify Everything is in Same Workspace:**
   - Pipeline, notebooks, and SQL Database should all be in the same workspace
   - Check workspace name in each artifact's settings

3. **Test the Notebook Directly:**
   - Open LogPipelineExecution notebook
   - Set test parameters manually:
     ```python
     execution_id = 'test-123'
     job_name = 'Test'
     action = 'start'
     ```
   - Run it cell by cell
   - If it fails → permissions issue confirmed
   - If it works → check pipeline configuration
