CREATE VIEW [dbo].[vw_refresh_cutoff]
AS
-- Resolves the cutoff that bounds "the current refresh cycle". When dbo.refresh_cutoff holds an
-- override row (set at orchestration start when useCurrentTimeStamp is enabled) that value wins;
-- otherwise it falls back to yesterday 22:00 UTC, the historical fixed boundary. All timestamps UTC.
SELECT
    COALESCE(
        (SELECT TOP (1) rc.cutoff_utc FROM dbo.refresh_cutoff AS rc ORDER BY rc.cutoff_utc DESC),
        DATEADD(HOUR, 22, CAST(DATEADD(DAY, -1, CAST(GETUTCDATE() AS DATE)) AS DATETIME2(7)))
    ) AS cutoff_utc;

GO

