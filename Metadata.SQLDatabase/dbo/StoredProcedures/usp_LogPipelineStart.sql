-- =============================================
-- Microsoft Fabric - Pipeline Execution Logging Stored Procedures
-- Purpose: Reusable logging for all pipelines
-- Database: Metadata SQL Database
-- =============================================

-- =============================================
-- Stored Procedure: usp_LogPipelineStart
-- Description: Logs the start of a pipeline execution
-- Parameters:
--   @execution_id: Pipeline run ID (from pipeline().RunId)
--   @job_name: Pipeline name (from pipeline().PipelineName)
-- =============================================
CREATE   PROCEDURE dbo.usp_LogPipelineStart
    @execution_id NVARCHAR(100),
    @job_name NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @current_time DATETIME2 = GETUTCDATE();
    
    -- Insert execution start
    INSERT INTO dbo.executions (execution_id, job_name, start_time, created_at)
    VALUES (@execution_id, @job_name, @current_time, @current_time);
    
    -- MERGE to jobs table - record start time
    MERGE dbo.jobs AS target
    USING (
        SELECT 
            @execution_id AS execution_id,
            @job_name AS job_name,
            @current_time AS start_time,
            @current_time AS created_at,
            'Pipeline' AS job_type,
            'Pipeline' AS object_type
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            execution_id = source.execution_id,
            start_time = source.start_time,
            end_time = NULL,
            result = NULL,
            error_message = NULL,
            created_at = source.created_at
    WHEN NOT MATCHED THEN
        INSERT (execution_id, job_name, start_time, end_time, result, error_message, created_at, job_type, object_type)
        VALUES (source.execution_id, source.job_name, source.start_time, NULL, NULL, NULL, source.created_at, source.job_type, source.object_type);
    
    -- Return success
    SELECT 
        @execution_id AS execution_id,
        @job_name AS job_name,
        @current_time AS logged_at,
        'START' AS action,
        'Success' AS status;
END

GO

