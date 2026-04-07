# Pipeline Configuration Guide

Step-by-step instructions for building the Load Sales pipeline with proper authentication settings.

## Common Issue: "Unauthorized" Error

If you're getting authentication errors when LogPipelineExecution runs from the pipeline, it's because the **notebook activity needs to run with workspace identity**.

## Load Sales Pipeline - Complete Configuration

### Activity 1: Log Start

**Activity Type:** Notebook

**Settings Tab:**
- **Notebook**: LogPipelineExecution

**Settings Tab (IMPORTANT for authentication):**
- Look for **"Authentication"** or **"Run as"** setting
- Select: **"Workspace identity"** or **"Default"**
- DO NOT select "User identity" or "Service principal" unless specifically configured

**Parameters:**
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

**Settings Tab:**
- Same authentication setting as Activity 1

**Parameters:**
- `rows_to_generate`: `100` (or your preferred value)

**On Success:** → Connect to "Log Success" activity
**On Failure:** → Connect to "Log Failure" activity

---

### Activity 3: Log Success

**Activity Type:** Notebook

**Settings Tab:**
- **Notebook**: LogPipelineExecution

**Settings Tab:**
- Same authentication setting as Activity 1

**Parameters:**
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

**Settings Tab:**
- Same authentication setting as Activity 1

**Parameters:**
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

**Try this:**

1. Open your pipeline in Fabric
2. Click on the failing notebook activity
3. Go to **Settings** tab
4. Look for authentication/identity options
5. Change to **"Workspace identity"** or **"Default"**
6. Save and re-run

**Alternative approach (if above doesn't work):**

Edit the notebook activity in pipeline JSON (Advanced editor):
```json
{
  "name": "Log Start",
  "type": "SynapseNotebook",
  "typeProperties": {
    "notebook": {
      "referenceName": "LogPipelineExecution",
      "type": "NotebookReference"
    },
    "parameters": {
      "execution_id": {
        "value": "@pipeline().RunId",
        "type": "Expression"
      },
      "job_name": {
        "value": "Load Sales",
        "type": "Expression"
      },
      "action": {
        "value": "start",
        "type": "Expression"
      }
    }
  }
}
```

Note: Ensure there's no explicit `sparkPool` or identity configuration that overrides workspace defaults.
