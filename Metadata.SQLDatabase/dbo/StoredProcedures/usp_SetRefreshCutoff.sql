CREATE PROCEDURE dbo.usp_SetRefreshCutoff
    @cutoff_utc DATETIME2(7)
AS
BEGIN
    SET NOCOUNT ON;

    -- The override table holds at most one row: replace whatever is there with the supplied cutoff
    -- (the orchestrator passes its own start time so the cycle begins at "now" instead of 22:00).
    -- DELETE (not TRUNCATE) so the db_datawriter connection can run it without ALTER permission.
    DELETE FROM dbo.refresh_cutoff;
    INSERT INTO dbo.refresh_cutoff (cutoff_utc) VALUES (@cutoff_utc);
END

GO

