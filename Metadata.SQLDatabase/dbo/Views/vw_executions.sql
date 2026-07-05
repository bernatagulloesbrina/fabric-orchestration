
CREATE VIEW [dbo].[vw_executions] AS
SELECT
    execution_id,
    job_name,
    CAST(start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time' AS DATETIME2(7)) AS start_time,
    CAST(end_time   AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time' AS DATETIME2(7)) AS end_time,
    CAST(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time' AS DATETIME2(7)) AS created_at,
    result,
    error_message
FROM dbo.executions;

GO

