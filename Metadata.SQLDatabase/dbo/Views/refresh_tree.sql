CREATE VIEW dbo.refresh_tree as

WITH dependency_tree AS (

    -- Roots: jobs with no prerequisites
    SELECT
        rj.job_name,
        rj.object_name,
        rj.object_type,
        rj.workspace_name,
        rj.priority,
        0                                   AS level,
        CAST(rj.job_name AS NVARCHAR(MAX)) AS full_path,
        CAST(rj.job_name AS NVARCHAR(MAX)) AS visited
    FROM dbo.refresh_jobs rj
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.refresh_job_precedence WHERE job_name = rj.job_name
    )

    UNION ALL

    -- Recursive: jobs that depend on the previous level
    SELECT
        rj.job_name,
        rj.object_name,
        rj.object_type,
        rj.workspace_name,
        rj.priority,
        dt.level + 1,
        CAST(dt.full_path + ' > ' + rj.job_name AS NVARCHAR(MAX)),
        CAST(dt.visited  + '|'  + rj.job_name AS NVARCHAR(MAX))
    FROM dbo.refresh_jobs rj
    INNER JOIN dbo.refresh_job_precedence rjp ON rj.job_name   = rjp.job_name
    INNER JOIN dependency_tree            dt  ON dt.job_name   = rjp.precedent_job_name
    WHERE dt.visited NOT LIKE '%|' + rj.job_name + '%'  -- cycle guard

)
SELECT
    level,
    job_name,
    REPLICATE('  · ', level) + object_name  AS indented_name,
    object_type,
    workspace_name,
    priority,
    full_path
FROM dependency_tree
--ORDER BY full_path, level;
;

GO

