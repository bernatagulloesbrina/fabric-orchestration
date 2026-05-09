CREATE VIEW [dbo].[error_jobs]
AS
WITH cutoff AS (
    SELECT DATEADD(HOUR, 22, CAST(DATEADD(DAY, -1, CAST(GETUTCDATE() AS DATE)) AS DATETIME2(7))) AS cutoff_utc
)
SELECT
    e.job_name,
    MAX(e.end_time) AS last_success_end_time,
    COUNT_BIG(*) AS successful_executions_since_cutoff
FROM dbo.executions AS e
CROSS JOIN cutoff AS c
WHERE e.result = 'Error'
  AND e.end_time >= c.cutoff_utc
GROUP BY e.job_name;

GO

