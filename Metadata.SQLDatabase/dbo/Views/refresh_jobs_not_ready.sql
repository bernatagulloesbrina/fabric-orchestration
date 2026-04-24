CREATE VIEW [dbo].[refresh_jobs_not_ready]
AS
SELECT
    pr.job_name,
    pr.workspace_id,
    pr.workspace_name,
    pr.object_type,
    pr.object_id,
    pr.object_name,
    pr.priority
FROM dbo.pending_refreshes AS pr
WHERE EXISTS (
    SELECT 1
    FROM dbo.refresh_job_precedence AS p
    LEFT JOIN dbo.completed_jobs AS cj
        ON cj.job_name = p.precedent_job_name
    WHERE p.job_name = pr.job_name
      AND cj.job_name IS NULL
);

GO
