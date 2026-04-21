
-- =============================================
-- Stored Procedure: usp_LogPipelineFailure
-- Description: Logs failure of a pipeline execution
-- Parameters:
--   @execution_id: Pipeline run ID
--   @job_name: Pipeline name
--   @error_message: Error details from failed activity
-- =============================================
CREATE   PROCEDURE dbo.usp_LogPipelineFailure
    @execution_id NVARCHAR(100),
    @job_name NVARCHAR(255),
    @error_message NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @current_time DATETIME2 = GETUTCDATE();
    
    -- Update execution end with error
    UPDATE dbo.executions
    SET 
        end_time = @current_time, 
        result = 'Failed',
        error_message = @error_message
    WHERE execution_id = @execution_id;
    
    -- MERGE to jobs table
    MERGE dbo.jobs AS target
    USING (
        SELECT 
            @job_name AS job_name,
            @current_time AS last_end_time,
            'Failed' AS last_result,
            @error_message AS error_message,
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
    
    -- Return failure info
    SELECT 
        @execution_id AS execution_id,
        @job_name AS job_name,
        @current_time AS logged_at,
        'FAILURE' AS action,
        'Logged' AS status,
        @error_message AS error_details;
END

GO

