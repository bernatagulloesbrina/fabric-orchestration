import fabric.functions as fn
import logging
import requests
from azure.identity import ManagedIdentityCredential

udf = fn.UserDataFunctions()

_JOB_TYPE_MAP = {
    "Dataflow": "Refresh",
    "DataPipeline": "Pipeline",
    "CopyJob": "Execute",
    "SemanticModel": "DefaultJob",
    "Notebook": "RunNotebook",
}


@udf.connection(argName="metadataSql", alias="Metadata")
@udf.function()
def trigger_on_demand_refresh(
    metadataSql: fn.FabricSqlConnection,
    jobName: str,
    modifiedBy: str,
    activeMarker: str,
) -> str:
    """
    Summary: Trigger a Fabric item refresh on demand.
    Description: Looks up workspace_id, object_id and object_type from dbo.refresh_jobs,
    then fires an on-demand refresh via the Fabric REST API. Fire-and-forget —
    does not wait for the refresh to complete.
    activeMarker must be non-null/non-empty (driven by the active_marker computed column)
    so that the Taskflow button is disabled for deleted jobs.
    """
    if not isinstance(jobName, str) or not jobName.strip():
        raise fn.UserThrownError("jobName is required and must be a non-empty string.", {"jobName": jobName})
    if not isinstance(modifiedBy, str) or not modifiedBy.strip():
        raise fn.UserThrownError("modifiedBy is required and must be a non-empty string.", {"modifiedBy": modifiedBy})
    if not activeMarker or not activeMarker.strip():
        raise fn.UserThrownError("This job is deleted and cannot be refreshed.", {"jobName": jobName})

    job_name = jobName.strip()
    modified_by = modifiedBy.strip()

    connection = metadataSql.connect()
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT workspace_id, object_id, object_type FROM dbo.refresh_jobs WHERE job_name = ? AND deleted IS NULL",
            (job_name,),
        )
        row = cursor.fetchone()
    finally:
        cursor.close()
        connection.close()

    if not row:
        raise fn.UserThrownError("Job not found.", {"jobName": job_name})

    workspace_id, object_id, object_type = row
    job_type = _JOB_TYPE_MAP.get(object_type, object_type)

    token = ManagedIdentityCredential().get_token("https://api.fabric.microsoft.com/.default").token

    url = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
        f"/items/{object_id}/jobs/instances?jobType={job_type}"
    )
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={},
        timeout=30,
    )

    if resp.status_code not in (200, 202):
        raise fn.UserThrownError(
            "Failed to trigger refresh.",
            {"jobName": job_name, "statusCode": resp.status_code, "response": resp.text[:500]},
        )

    logging.info("On-demand refresh triggered for %s by %s", job_name, modified_by)
    return f"Refresh triggered for '{job_name}' by {modified_by}."
