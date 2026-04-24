
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
            @execution_id AS execution_id,
            @job_name AS job_name,
            @current_time AS end_time,
            'Success' AS result,
            NULL AS error_message,
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
    
    -- Return success
    SELECT 
        @execution_id AS execution_id,
        @job_name AS job_name,
        @current_time AS logged_at,
        'SUCCESS' AS action,
        'Logged' AS status;
END

GO

