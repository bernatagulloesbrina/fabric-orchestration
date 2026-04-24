CREATE VIEW [dbo].[refresh_jobs_ready]
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
LEFT JOIN dbo.refresh_jobs_not_ready AS nr
    ON nr.job_name = pr.job_name
WHERE nr.job_name IS NULL;

GO
