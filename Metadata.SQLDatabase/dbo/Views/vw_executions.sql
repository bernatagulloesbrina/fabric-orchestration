
CREATE VIEW [dbo].[vw_executions] AS
SELECT
    e.execution_id,
    e.job_name,
    CAST(e.start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time' AS DATETIME2(7)) AS start_time,
    CAST(e.end_time   AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time' AS DATETIME2(7)) AS end_time,
    CAST(e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time' AS DATETIME2(7)) AS created_at,
    CAST(CAST(e.start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time' AS DATETIME2(7)) AS DATE) AS start_date,
    CAST(
        DATEADD(
            HOUR,
            DATEDIFF(HOUR, 0, CAST(e.start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Tokyo Standard Time' AS DATETIME2(7))),
            0
        ) AS DATETIME2(7)
    ) AS start_hour,
    j.object_type,
    e.result,
    e.error_message
FROM dbo.executions e
LEFT JOIN dbo.jobs j ON j.job_name = e.job_name;

GO

