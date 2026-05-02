import fabric.functions as fn
import logging

udf = fn.UserDataFunctions()


@udf.connection(argName="metadataSql", alias="Metadata")
@udf.function()
def delete_refresh_job(
    metadataSql: fn.FabricSqlConnection,
    jobName: str,
    modifiedBy: str,
    deleted: str,        
) -> str:
    if deleted is not None:
        raise fn.UserThrownError(
            "This job is already deleted.",
            {"jobName": jobName, "deleted": deleted},
        )
    """
    Summary: Soft-delete a refresh job.
    Description: Sets deleted to today's UTC date and records who made the change.
    """
    if not isinstance(jobName, str) or not jobName.strip():
        raise fn.UserThrownError("jobName is required and must be a non-empty string.", {"jobName": jobName})
    if not isinstance(modifiedBy, str) or not modifiedBy.strip():
        raise fn.UserThrownError("modifiedBy is required and must be a non-empty string.", {"modifiedBy": modifiedBy})

    sql = """
    UPDATE dbo.refresh_jobs
    SET    deleted          = CAST(GETUTCDATE() AS DATE),
           last_modified_by = ?,
           last_modified_on = GETUTCDATE()
    WHERE  job_name = ?;
    """

    connection = metadataSql.connect()
    cursor = connection.cursor()

    try:
        cursor.execute(sql, (modifiedBy.strip(), jobName.strip()))
        connection.commit()
    except Exception as exc:
        connection.rollback()
        logging.exception("Failed to delete refresh job %s", jobName)
        raise fn.UserThrownError(
            "Failed to soft-delete refresh job.",
            {"jobName": jobName, "error": str(exc)},
        ) from exc
    finally:
        cursor.close()
        connection.close()

    return f"Refresh job '{jobName.strip()}' marked as deleted by {modifiedBy.strip()}."
