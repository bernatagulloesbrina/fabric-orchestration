CREATE VIEW dbo.vw_timeline_axis AS
WITH Digits AS (
    SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
    UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
),
Numbers AS (
    SELECT (d4.n * 1000 + d3.n * 100 + d2.n * 10 + d1.n) AS n
    FROM Digits d1
    CROSS JOIN Digits d2
    CROSS JOIN Digits d3
    CROSS JOIN Digits d4
)
SELECT
    CAST(
        DATEADD(
            MINUTE,
            -1439 + n,
            CAST(SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central European Standard Time' AS DATETIME2(7))
        )
        AS DATETIME2(7)
    ) AS axis_datetime
FROM Numbers
WHERE n <= 1439;

GO
