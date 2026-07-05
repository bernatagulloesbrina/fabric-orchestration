CREATE VIEW [dbo].[refresh_jobs_ready]
AS
WITH ready AS (
    -- Pending refreshes whose precedents have all completed. The retry cap (max 3 attempts) is
    -- already applied upstream in pending_refreshes, so nothing exhausted reaches here.
    SELECT
        pr.job_name,
        pr.workspace_id,
        pr.workspace_name,
        pr.object_type,
        pr.object_id,
        pr.object_name,
        pr.priority,
        pr.attempt_count
    FROM dbo.pending_refreshes AS pr
    LEFT JOIN dbo.refresh_jobs_not_ready AS nr
        ON nr.job_name = pr.job_name
    WHERE nr.job_name IS NULL
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
        r.attempt_count,
        CASE WHEN sp.job_name IS NOT NULL THEN 1 ELSE 0 END AS is_sharepoint
    FROM ready AS r
    LEFT JOIN sharepoint_jobs AS sp
        ON sp.job_name = r.job_name
),
sharepoint_ranked AS (
    -- Keep only the top two SharePoint-dependent jobs, ordered by FAILED attempts then priority.
    -- attempt_count is failed-only (from pending_refreshes), so in-flight / never-run jobs sit at
    -- 0 alongside fresh ones and keep their slots (the throttle stays stable) -- only a job that
    -- actually failed (attempt_count >= 1) is pushed to the back, yielding its slot to others.
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
-- pushes attempted-and-failed jobs to the back of the processing order.
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

