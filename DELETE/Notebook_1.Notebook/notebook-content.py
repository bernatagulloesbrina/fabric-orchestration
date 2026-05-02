# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
import requests

# 1) Get access token
# Preferred in Fabric notebooks: use notebook runtime identity token ("pbi" audience)
try:
    from notebookutils import mssparkutils
    access_token = mssparkutils.credentials.getToken("pbi")
    print("Token acquired from Fabric notebook runtime.")
except Exception as e:
    print(f"Notebook token failed: {e}")
    print("Falling back to DeviceCodeCredential...")

    from azure.identity import DeviceCodeCredential
    cred = DeviceCodeCredential()
    # For Power BI/Fabric APIs, use .default scope
    access_token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
    print("Token acquired via device code flow.")

if not access_token:
    raise RuntimeError("Could not acquire access token.")

# 2) Prepare request
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

endpoint = "https://d1cce96c953e4c7e8bc36f6e375f304c.zd1.userdatafunctions.fabric.microsoft.com/v1/workspaces/d1cce96c-953e-4c7e-8bc3-6f6e375f304c/userDataFunctions/c02bc5f0-9827-4fb2-88ab-8d1dd610801f/functions/create_or_replace_refresh/invoke"

request_body = {
    "jobName": "Load Sales",
    "workspaceId": "d1cce96c-953e-4c7e-8bc3-6f6e375f304c",
    "workspaceName": "Fabric Orchestration",
    "objectType": "SemanticModel",
    "objectId": "11111111-1111-1111-1111-111111111111",
    "objectName": "Sales Model",
    "priority": 1,
    "precedentJobNames": ""
}

# 3) Invoke function
response = requests.post(endpoint, headers=headers, json=request_body, timeout=60)

print("Status:", response.status_code)
try:
    print(json.dumps(response.json(), indent=2))
except Exception:
    print(response.text)

response.raise_for_status()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
