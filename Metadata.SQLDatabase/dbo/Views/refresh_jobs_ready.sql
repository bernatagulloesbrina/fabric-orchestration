CREATE VIEW [dbo].[refresh_jobs_ready]
AS
WITH ready AS (
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
sharepoint_jobs AS (
    -- Jobs that read from SharePoint. refresh_job_sources lives in the MetadataLakehouse,
    -- referenced here via three-part (cross-database) naming within the same workspace.
    SELECT DISTINCT s.job_name
    FROM [MetadataLakehouse].[dbo].[refresh_job_sources] AS s
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
        CASE WHEN sp.job_name IS NOT NULL THEN 1 ELSE 0 END AS is_sharepoint
    FROM ready AS r
    LEFT JOIN sharepoint_jobs AS sp
        ON sp.job_name = r.job_name
),
sharepoint_ranked AS (
    -- Rank SharePoint-dependent ready jobs so we can keep only the top two.
    SELECT
        f.job_name,
        f.workspace_id,
        f.workspace_name,
        f.object_type,
        f.object_id,
        f.object_name,
        f.priority,
        ROW_NUMBER() OVER (ORDER BY f.priority ASC, f.job_name ASC) AS sp_rank
    FROM flagged AS f
    WHERE f.is_sharepoint = 1
)
-- All ready jobs that do NOT depend on SharePoint.
SELECT
    job_name,
    workspace_id,
    workspace_name,
    object_type,
    object_id,
    object_name,
    priority
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
    priority
FROM sharepoint_ranked
WHERE sp_rank <= 2;

GO
