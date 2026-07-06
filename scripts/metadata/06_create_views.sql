-- ============================================================
-- Target : Metadata (Fabric SQL Database)
-- Purpose: Reporting views that convert UTC timestamps to
--          Central European Time (CET/CEST, UTC+1/UTC+2).
--          DST is applied automatically per timestamp using
--          AT TIME ZONE 'Central European Standard Time'.
-- ============================================================

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

CREATE VIEW dbo.vw_executions AS
SELECT
    e.execution_id,
    e.job_name,
    CAST(e.start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS start_time,
    CAST(e.end_time   AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS end_time,
    CAST(e.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS created_at,
    CAST(CAST(e.start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS DATE) AS start_date,
    CAST(
        DATEADD(
            HOUR,
            DATEDIFF(HOUR, 0, CAST(e.start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7))),
            0
        ) AS TIME(0)
    ) AS start_hour,
    j.object_type,
    e.result,
    e.error_message
FROM dbo.executions e
LEFT JOIN dbo.jobs j ON j.job_name = e.job_name;
GO

CREATE VIEW dbo.vw_refresh_jobs AS
SELECT
    job_name,
    workspace_id,
    workspace_name,
    object_type,
    object_id,
    object_name,
    priority,
    deleted,
    last_modified_by,
    CAST(last_modified_on AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS last_modified_on
FROM dbo.refresh_jobs;
GO
