CREATE PROCEDURE dbo.usp_ClearRefreshCutoff
AS
BEGIN
    SET NOCOUNT ON;

    -- Empty the override table so subsequent runs fall back to the default yesterday-22:00 boundary.
    -- DELETE (not TRUNCATE) so the db_datawriter connection can run it without ALTER permission.
    DELETE FROM dbo.refresh_cutoff;
END

GO

