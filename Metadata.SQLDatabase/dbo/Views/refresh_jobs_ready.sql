CREATE VIEW [dbo].[refresh_jobs_ready]
AS
WITH cutoff AS (
    -- Start of the current refresh cycle (yesterday 22:00 UTC), same boundary as completed_jobs.
    SELECT DATEADD(HOUR, 22, CAST(DATEADD(DAY, -1, CAST(GETUTCDATE() AS DATE)) AS DATETIME2(7))) AS cutoff_utc
),
ready AS (
    -- Pending refreshes whose precedents have all completed (original "ready" logic).
    SELECT
        pr.job_name,
        pr.workspace_id,
        pr.workspace_name,
        pr.object_type,
        pr.object_id,
        pr.object_name,
        pr.priority
    FROM dbo.pending_refreshes AS pr
    LEFT JOIN dbo.refresh_jobs_not_ready AS nr
        ON nr.job_name = pr.job_name
    WHERE nr.job_name IS NULL
),
attempts AS (
    -- Failed attempts in the current cycle, per job. Used to push retried-and-failed jobs
    -- to the back of the queue so untried jobs get their turn first.
    SELECT
        e.job_name,
        COUNT_BIG(*) AS attempt_count
    FROM dbo.executions AS e
    CROSS JOIN cutoff AS c
    WHERE e.result = 'Error'
      AND e.start_time >= c.cutoff_utc
    GROUP BY e.job_name
),
sharepoint_jobs AS (
    -- Jobs that read from SharePoint. Sourced from dbo.refresh_job_sources, a local copy of the
    -- lakehouse table reloaded each run (the transactional SQL DB cannot cross-query the lakehouse).
    SELECT DISTINCT s.job_name
    FROM dbo.refresh_job_sources AS s
    WHERE s.datasource_type LIKE 'SharePoint%'
),
flagged AS (
    SELECT
        r.job_name,
        r.workspace_id,
        r.workspace_name,
        r.object_type,
        r.object_id,
        r.object_name,
        r.priority,
        CASE WHEN sp.job_name IS NOT NULL THEN 1 ELSE 0 END AS is_sharepoint,
        COALESCE(a.attempt_count, 0) AS attempt_count
    FROM ready AS r
    LEFT JOIN sharepoint_jobs AS sp
        ON sp.job_name = r.job_name
    LEFT JOIN attempts AS a
        ON a.job_name = r.job_name
),
sharepoint_ranked AS (
    -- Rank SharePoint-dependent ready jobs so we can keep only the top two.
    -- Order by failed attempts first (ascending) so jobs that already failed yield to untried
    -- jobs, then by priority.
    SELECT
        f.job_name,
        f.workspace_id,
        f.workspace_name,
        f.object_type,
        f.object_id,
        f.object_name,
        f.priority,
        f.attempt_count,
        ROW_NUMBER() OVER (ORDER BY f.attempt_count ASC, f.priority ASC, f.job_name ASC) AS sp_rank
    FROM flagged AS f
    WHERE f.is_sharepoint = 1
)
-- attempt_count is exposed so the consumer can ORDER BY attempt_count, priority -- this is what
-- pushes attempted-and-failed jobs to the back of the queue (for SharePoint and the rest alike).
-- All ready jobs that do NOT depend on SharePoint.
SELECT
    job_name,
    workspace_id,
    workspace_name,
    object_type,
    object_id,
    object_name,
    priority,
    attempt_count
FROM flagged
WHERE is_sharepoint = 0
UNION ALL
-- Only the top two ready jobs that DO depend on SharePoint (throttle).
SELECT
    job_name,
    workspace_id,
    workspace_name,
    object_type,
    object_id,
    object_name,
    priority,
    attempt_count
FROM sharepoint_ranked
WHERE sp_rank <= 2;

GO
