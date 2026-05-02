CREATE PROCEDURE dbo.usp_RegisterRefresh
    @execution_id  NVARCHAR(50),
    @job_name      NVARCHAR(200),
    @start_time    DATETIME2(7),
    @end_time      DATETIME2(7)   = NULL,
    @result        NVARCHAR(50)   = NULL,
    @error_message NVARCHAR(MAX)  = NULL,
    @job_type      NVARCHAR(50)   = NULL,
    @object_type   NVARCHAR(50)   = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @current_time DATETIME2 = GETUTCDATE();

    -- Upsert executions on (job_name, start_time)
    MERGE dbo.executions AS target
    USING (
        SELECT
            @execution_id  AS execution_id,
            @job_name      AS job_name,
            @start_time    AS start_time,
            @end_time      AS end_time,
            @result        AS result,
            @error_message AS error_message,
            @job_type      AS job_type,
            @object_type   AS object_type,
            @current_time  AS created_at
    ) AS source
    ON target.job_name = source.job_name AND target.start_time = source.start_time
    WHEN MATCHED THEN
        UPDATE SET
            execution_id  = source.execution_id,
            end_time      = source.end_time,
            result        = source.result,
            error_message = source.error_message,
            job_type      = source.job_type,
            object_type   = source.object_type
    WHEN NOT MATCHED THEN
        INSERT (execution_id, job_name, start_time, end_time, result, error_message, job_type, object_type, created_at)
        VALUES (source.execution_id, source.job_name, source.start_time, source.end_time, source.result, source.error_message, source.job_type, source.object_type, source.created_at);

    -- Upsert jobs on job_name
    MERGE dbo.jobs AS target
    USING (
        SELECT
            @execution_id  AS execution_id,
            @job_name      AS job_name,
            @start_time    AS start_time,
            @end_time      AS end_time,
            @result        AS result,
            @error_message AS error_message,
            @job_type      AS job_type,
            @object_type   AS object_type,
            @current_time  AS created_at
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            execution_id  = source.execution_id,
            start_time    = source.start_time,
            end_time      = source.end_time,
            result        = source.result,
            error_message = source.error_message,
            job_type      = COALESCE(source.job_type, target.job_type),
            object_type   = COALESCE(source.object_type, target.object_type)
    WHEN NOT MATCHED THEN
        INSERT (execution_id, job_name, start_time, end_time, result, error_message, job_type, object_type, created_at)
        VALUES (source.execution_id, source.job_name, source.start_time, source.end_time, source.result, source.error_message, source.job_type, source.object_type, source.created_at);

    SELECT
        @execution_id AS execution_id,
        @job_name     AS job_name,
        @start_time   AS start_time,
        @current_time AS logged_at;
END

GO
