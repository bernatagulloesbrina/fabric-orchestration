

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

