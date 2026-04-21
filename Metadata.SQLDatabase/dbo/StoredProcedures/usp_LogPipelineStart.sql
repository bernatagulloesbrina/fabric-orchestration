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
            @job_name AS job_name,
            @current_time AS last_start_time,
            @current_time AS updated_at
    ) AS source
    ON target.job_name = source.job_name
    WHEN MATCHED THEN
        UPDATE SET
            last_start_time = source.last_start_time,
            updated_at = source.updated_at
    WHEN NOT MATCHED THEN
        INSERT (job_name, last_start_time, updated_at)
        VALUES (source.job_name, source.last_start_time, source.updated_at);
    
    -- Return success
    SELECT 
        @execution_id AS execution_id,
        @job_name AS job_name,
        @current_time AS logged_at,
        'START' AS action,
        'Success' AS status;
END

GO

