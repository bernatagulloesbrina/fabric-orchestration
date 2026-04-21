
-- =============================================
-- Stored Procedure: usp_LogPipelineSuccess
-- Description: Logs successful completion of a pipeline
-- Parameters:
--   @execution_id: Pipeline run ID
--   @job_name: Pipeline name
-- =============================================
CREATE   PROCEDURE dbo.usp_LogPipelineSuccess
    @execution_id NVARCHAR(100),
    @job_name NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @current_time DATETIME2 = GETUTCDATE();
    
    -- Update execution end
    UPDATE dbo.executions
    SET 
        end_time = @current_time, 
        result = 'Success'
    WHERE execution_id = @execution_id;
    
    -- MERGE to jobs table
    MERGE dbo.jobs AS target
    USING (
        SELECT 
            @job_name AS job_name,
            @current_time AS last_end_time,
            'Success' AS last_result,
            NULL AS error_message,
            @current_time AS updated_at
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            last_end_time = source.last_end_time,
            last_result = source.last_result,
            error_message = source.error_message,
            updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (job_name, last_end_time, last_result, error_message, updated_at)
        VALUES (source.job_name, source.last_end_time, source.last_result, source.error_message, source.updated_at);
    
    -- Return success
    SELECT 
        @execution_id AS execution_id,
        @job_name AS job_name,
        @current_time AS logged_at,
        'SUCCESS' AS action,
        'Logged' AS status;
END

GO

