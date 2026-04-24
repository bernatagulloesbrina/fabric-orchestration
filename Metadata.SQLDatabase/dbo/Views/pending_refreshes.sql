CREATE VIEW [dbo].[pending_refreshes]
AS
SELECT
    rj.job_name,
    rj.workspace_id,
    rj.workspace_name,
    rj.object_type,
    rj.object_id,
    rj.object_name,
    rj.priority
FROM dbo.refresh_jobs AS rj
LEFT JOIN dbo.completed_jobs AS cj
    ON cj.job_name = rj.job_name
WHERE cj.job_name IS NULL;

GO
