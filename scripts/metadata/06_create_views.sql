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
    execution_id,
    job_name,
    CAST(start_time AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS start_time,
    CAST(end_time   AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS end_time,
    CAST(created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7)) AS created_at,
    result,
    error_message
FROM dbo.executions;
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

CREATE VIEW dbo.vw_timeline_axis AS
WITH Digits AS (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
),
Numbers AS (
    SELECT (d4.n * 1000 + d3.n * 100 + d2.n * 10 + d1.n) AS n
    FROM Digits d1
    CROSS JOIN Digits d2
    CROSS JOIN Digits d3
    CROSS JOIN Digits d4
)
SELECT
    CAST(
        DATEADD(
            MINUTE,
            -1439 + n,
            CAST(SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7))
        )
        AS DATETIME2(7)
    ) AS axis_datetime
FROM Numbers
WHERE n <= 1439;
GO
