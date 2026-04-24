
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
            @execution_id AS execution_id,
            @job_name AS job_name,
            @current_time AS end_time,
            'Failed' AS result,
            @error_message AS error_message,
            @current_time AS created_at,
            'Pipeline' AS job_type,
            'Pipeline' AS object_type
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            execution_id = source.execution_id,
            end_time = source.end_time,
            result = source.result,
            error_message = source.error_message,
            created_at = source.created_at
    WHEN NOT MATCHED THEN
        INSERT (execution_id, job_name, start_time, end_time, result, error_message, created_at, job_type, object_type)
        VALUES (source.execution_id, source.job_name, NULL, source.end_time, source.result, source.error_message, source.created_at, source.job_type, source.object_type);
    
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

