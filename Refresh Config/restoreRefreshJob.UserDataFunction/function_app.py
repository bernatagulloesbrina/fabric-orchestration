import fabric.functions as fn
import logging
from datetime import date

udf = fn.UserDataFunctions()


def _is_date(value: str) -> bool:
    if not value or not isinstance(value, str) or not value.strip():
        return False
    try:
        date.fromisoformat(value.strip())
        return True
    except ValueError:
        return False


@udf.connection(argName="metadataSql", alias="Metadata")
@udf.function()
def restore_refresh_job(
    metadataSql: fn.FabricSqlConnection,
    jobName: str,
    modifiedBy: str,
    deleted: str,
) -> str:
    """
    Summary: Restore a soft-deleted refresh job.
    Description: Clears the deleted date and records who made the change.
    """
    if not isinstance(jobName, str) or not jobName.strip():
        raise fn.UserThrownError("jobName is required and must be a non-empty string.", {"jobName": jobName})
    if not isinstance(modifiedBy, str) or not modifiedBy.strip():
        raise fn.UserThrownError("modifiedBy is required and must be a non-empty string.", {"modifiedBy": modifiedBy})
    if not _is_date(deleted):
        raise fn.UserThrownError("This job is not deleted, nothing to restore.", {"jobName": jobName})

    sql = """
    UPDATE dbo.refresh_jobs
    SET    deleted          = NULL,
           last_modified_by = ?,
           last_modified_on = CAST(GETUTCDATE() AS DATETIME2(0))
    WHERE  job_name = ?;
    """

    connection = metadataSql.connect()
    cursor = connection.cursor()

    try:
        cursor.execute(sql, (modifiedBy.strip(), jobName.strip()))
        connection.commit()
    except Exception as exc:
        connection.rollback()
        logging.exception("Failed to restore refresh job %s", jobName)
        raise fn.UserThrownError(
            "Failed to restore refresh job.",
            {"jobName": jobName, "error": str(exc)},
        ) from exc
    finally:
        cursor.close()
        connection.close()

    return f"Refresh job '{jobName.strip()}' restored by {modifiedBy.strip()}."
