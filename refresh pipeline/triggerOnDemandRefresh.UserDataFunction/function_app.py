import fabric.functions as fn
import logging
import requests
from azure.identity import ClientSecretCredential

udf = fn.UserDataFunctions()


_PIPELINE_NAME = "09_Refresh Fabric Item On Demand"


def _get_token(config: dict) -> str:
    return ClientSecretCredential(
        tenant_id=config["SP_TENANT_ID"],
        client_id=config["SP_CLIENT_ID"],
        client_secret=config["SP_CLIENT_SECRET"],
    ).get_token("https://api.fabric.microsoft.com/.default").token


def _find_workspace_id(workspace_name: str, token: str) -> str:
    resp = requests.get(
        "https://api.fabric.microsoft.com/v1/workspaces",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise fn.UserThrownError(f"Failed to list workspaces. HTTP {resp.status_code}: {resp.text[:300]}", {})
    match = next((w for w in resp.json().get("value", []) if w["displayName"] == workspace_name), None)
    if not match:
        raise fn.UserThrownError(f"Workspace '{workspace_name}' not found.", {})
    return match["id"]


def _find_pipeline_id(workspace_id: str, pipeline_name: str, token: str) -> str:
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items?type=DataPipeline",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise fn.UserThrownError(f"Failed to list pipelines. HTTP {resp.status_code}: {resp.text[:300]}", {})
    match = next((i for i in resp.json().get("value", []) if i["displayName"] == pipeline_name), None)
    if not match:
        raise fn.UserThrownError(f"Pipeline '{pipeline_name}' not found in workspace.", {})
    return match["id"]


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
    Description: Looks up the job from dbo.refresh_jobs and fires pipeline
    09_Refresh Fabric Item On Demand with the job parameters. The pipeline
    handles the refresh, waits for completion, and registers the result.
    """
    try:
        if not isinstance(jobName, str) or not jobName.strip():
            raise fn.UserThrownError("jobName is required.", {"jobName": jobName})
        if not isinstance(modifiedBy, str) or not modifiedBy.strip():
            raise fn.UserThrownError("modifiedBy is required.", {"modifiedBy": modifiedBy})
        if not activeMarker or not activeMarker.strip():
            raise fn.UserThrownError("This job is deleted and cannot be refreshed.", {"jobName": jobName})

        job_name = jobName.strip()
        modified_by = modifiedBy.strip()

        connection = metadataSql.connect()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT config_key, config_value FROM dbo.udf_config WHERE config_key IN "
                "('SP_TENANT_ID', 'SP_CLIENT_ID', 'SP_CLIENT_SECRET', 'PIPELINE_WORKSPACE_NAME')"
            )
            config = {row[0]: row[1] for row in cursor.fetchall()}

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

        token = _get_token(config)
        pipeline_workspace_id = _find_workspace_id(config["PIPELINE_WORKSPACE_NAME"], token)
        pipeline_id = _find_pipeline_id(pipeline_workspace_id, _PIPELINE_NAME, token)

        url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{pipeline_workspace_id}"
            f"/items/{pipeline_id}/jobs/instances?jobType=Pipeline"
        )
        resp = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"executionData": {"parameters": {
                "workspace_id": workspace_id,
                "object_id":    object_id,
                "object_type":  object_type,
                "job_name":     job_name,
            }}},
            timeout=30,
        )

        if resp.status_code not in (200, 202):
            raise fn.UserThrownError(
                f"Failed to trigger pipeline. HTTP {resp.status_code}: {resp.text[:300]}",
                {"jobName": job_name},
            )

        logging.info("On-demand refresh pipeline triggered for %s by %s", job_name, modified_by)
        return f"Refresh triggered for '{job_name}' by {modified_by}."

    except fn.UserThrownError:
        raise
    except Exception as exc:
        raise fn.UserThrownError(f"Unexpected error: {type(exc).__name__}: {exc}", {}) from exc
