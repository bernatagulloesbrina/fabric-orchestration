CREATE VIEW [dbo].[pending_refreshes]
AS
WITH attempts AS (
    -- Failed attempts (result = 'Error') per job in the current cycle, exposed so downstream
    -- ordering can push attempted-and-failed jobs to the back of the queue. The cycle boundary
    -- comes from dbo.vw_refresh_cutoff (override row or yesterday 22:00 UTC), same as completed_jobs.
    SELECT
        e.job_name,
        COUNT_BIG(CASE WHEN e.result = 'Error' THEN 1 END) AS failed_attempts
    FROM dbo.executions AS e
    CROSS JOIN dbo.vw_refresh_cutoff AS c
    WHERE e.start_time >= c.cutoff_utc
    GROUP BY e.job_name
)
SELECT
    rj.job_name,
    rj.workspace_id,
    rj.workspace_name,
    rj.object_type,
    rj.object_id,
    rj.object_name,
    rj.priority,
    COALESCE(a.failed_attempts, 0) AS attempt_count
FROM dbo.refresh_jobs AS rj
LEFT JOIN dbo.completed_jobs AS cj
    ON cj.job_name = rj.job_name
LEFT JOIN attempts AS a
    ON a.job_name = rj.job_name
WHERE cj.job_name IS NULL;

GO

