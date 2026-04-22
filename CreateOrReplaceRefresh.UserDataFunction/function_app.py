import fabric.functions as fn
import logging
from typing import Iterable

udf = fn.UserDataFunctions()


def _require_non_empty_string(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise fn.UserThrownError(f"{name} is required and must be a non-empty string.", {name: value})
    return value.strip()


def _normalize_dependencies(jobName: str, precedentJobNames: str | None) -> list[str]:
    """Parse a pipe-separated string of job names into a deduplicated list.

    Example input: "JobA|JobB|JobC"
    Returns: ["JobA", "JobB", "JobC"]
    """
    if not precedentJobNames or not precedentJobNames.strip():
        return []

    normalized: list[str] = []
    seen: set[str] = set()

    for raw_name in precedentJobNames.split("|"):
        dependency_name = raw_name.strip()
        if not dependency_name:
            continue
        if dependency_name == jobName:
            raise fn.UserThrownError(
                "A refresh job cannot depend on itself.",
                {"jobName": jobName, "precedentJobName": dependency_name},
            )
        if dependency_name in seen:
            continue
        seen.add(dependency_name)
        normalized.append(dependency_name)

    return normalized


def _execute_many(cursor, sql: str, rows: Iterable[tuple]) -> None:
    for row in rows:
        cursor.execute(sql, row)


@udf.connection(argName="metadataSql", alias="Metadata")
@udf.function()
def create_or_replace_refresh(
    metadataSql: fn.FabricSqlConnection,
    jobName: str,
    workspaceId: str,
    workspaceName: str,
    objectType: str,
    objectId: str,
    objectName: str,
    priority: int,
    precedentJobNames: str | None = None,
) -> str:
    """
    Summary: Create or replace a Fabric refresh job definition.
    Description: Upserts dbo.refresh_jobs, upserts dbo.jobs with Fabric job metadata,
    and replaces dbo.refresh_job_precedence entries for the provided job.
    Provide precedentJobNames as a pipe-separated string, e.g. "JobA|JobB|JobC".
    The connection alias metadataSqlDb must be configured in the Fabric item.
    """
    normalized_job_name = _require_non_empty_string(jobName, "jobName")
    normalized_workspace_id = _require_non_empty_string(workspaceId, "workspaceId")
    normalized_workspace_name = _require_non_empty_string(workspaceName, "workspaceName")
    normalized_object_type = _require_non_empty_string(objectType, "objectType")
    normalized_object_id = _require_non_empty_string(objectId, "objectId")
    normalized_object_name = _require_non_empty_string(objectName, "objectName")

    if not isinstance(priority, int):
        raise fn.UserThrownError("priority must be an integer.", {"priority": priority})

    dependencies = _normalize_dependencies(normalized_job_name, precedentJobNames)

    refresh_upsert_sql = """
    MERGE dbo.refresh_jobs AS target
    USING (
        SELECT
            ? AS job_name,
            ? AS workspace_id,
            ? AS workspace_name,
            ? AS object_type,
            ? AS object_id,
            ? AS object_name,
            ? AS priority
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            workspace_id = source.workspace_id,
            workspace_name = source.workspace_name,
            object_type = source.object_type,
            object_id = source.object_id,
            object_name = source.object_name,
            priority = source.priority
    WHEN NOT MATCHED THEN
        INSERT (job_name, workspace_id, workspace_name, object_type, object_id, object_name, priority)
        VALUES (
            source.job_name,
            source.workspace_id,
            source.workspace_name,
            source.object_type,
            source.object_id,
            source.object_name,
            source.priority
        );
    """

    jobs_upsert_sql = """
    MERGE dbo.jobs AS target
    USING (
        SELECT
            ? AS job_name,
            'Fabric' AS job_type,
            ? AS object_type
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            job_type = source.job_type,
            object_type = source.object_type
    WHEN NOT MATCHED THEN
        INSERT (job_name, job_type, object_type)
        VALUES (source.job_name, source.job_type, source.object_type);
    """

    delete_dependencies_sql = "DELETE FROM dbo.refresh_job_precedence WHERE job_name = ?;"
    insert_dependency_sql = """
    INSERT INTO dbo.refresh_job_precedence (job_name, precedent_job_name)
    VALUES (?, ?);
    """

    connection = metadataSql.connect()
    cursor = connection.cursor()

    try:
        logging.info("Upserting refresh job %s", normalized_job_name)

        cursor.execute(
            refresh_upsert_sql,
            (
                normalized_job_name,
                normalized_workspace_id,
                normalized_workspace_name,
                normalized_object_type,
                normalized_object_id,
                normalized_object_name,
                priority,
            ),
        )
        cursor.execute(jobs_upsert_sql, (normalized_job_name, normalized_object_type))
        cursor.execute(delete_dependencies_sql, normalized_job_name)

        if dependencies:
            _execute_many(
                cursor,
                insert_dependency_sql,
                [(normalized_job_name, dependency_name) for dependency_name in dependencies],
            )

        connection.commit()
    except Exception as exc:
        connection.rollback()
        logging.exception("Failed to create or replace refresh job %s", normalized_job_name)
        raise fn.UserThrownError(
            "Failed to create or replace refresh metadata.",
            {"jobName": normalized_job_name, "error": str(exc)},
        ) from exc
    finally:
        cursor.close()
        connection.close()

    return (
        f"Refresh '{normalized_job_name}' synchronized with {len(dependencies)} precedent "
        f"job(s). Connection alias: metadataSqlDb."
    )
