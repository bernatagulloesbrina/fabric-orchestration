CREATE VIEW [dbo].[error_jobs]
AS
SELECT
    e.job_name,
    MAX(e.end_time) AS last_success_end_time,
    COUNT_BIG(*) AS successful_executions_since_cutoff
FROM dbo.executions AS e
CROSS JOIN dbo.vw_refresh_cutoff AS c
WHERE e.result = 'Error'
  AND e.end_time >= c.cutoff_utc
GROUP BY e.job_name;

GO

