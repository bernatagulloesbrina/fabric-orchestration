CREATE VIEW dbo.vw_jobs AS
SELECT
    execution_id,
    job_name,
    job_type,
    object_type,
    CAST(start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS start_time,
    CAST(end_time   AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS end_time,
    CAST(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS created_at,
    result,
    error_message
FROM dbo.jobs;

GO

