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
CREATE OR ALTER PROCEDURE dbo.usp_LogPipelineStart
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

-- =============================================
-- Stored Procedure: usp_LogPipelineSuccess
-- Description: Logs successful completion of a pipeline
-- Parameters:
--   @execution_id: Pipeline run ID
--   @job_name: Pipeline name
-- =============================================
CREATE OR ALTER PROCEDURE dbo.usp_LogPipelineSuccess
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

-- =============================================
-- Stored Procedure: usp_LogPipelineFailure
-- Description: Logs failure of a pipeline execution
-- Parameters:
--   @execution_id: Pipeline run ID
--   @job_name: Pipeline name
--   @error_message: Error details from failed activity
-- =============================================
CREATE OR ALTER PROCEDURE dbo.usp_LogPipelineFailure
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

-- =============================================
-- Grant permissions to workspace identity
-- Note: Replace [Fabric Orchestration] with your workspace name
-- or the service principal/user that runs the pipelines
-- =============================================
-- GRANT EXECUTE ON dbo.usp_LogPipelineStart TO [Fabric Orchestration];
-- GRANT EXECUTE ON dbo.usp_LogPipelineSuccess TO [Fabric Orchestration];
-- GRANT EXECUTE ON dbo.usp_LogPipelineFailure TO [Fabric Orchestration];

PRINT 'Pipeline logging stored procedures created successfully!';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Uncomment and run the GRANT statements above (replace workspace name)';
PRINT '2. In your pipelines, use Script activities with these calls:';
PRINT '';
PRINT '   -- Log Start:';
PRINT '   EXEC dbo.usp_LogPipelineStart @execution_id = ''@{pipeline().RunId}'', @job_name = ''@{pipeline().PipelineName}'';';
PRINT '';
PRINT '   -- Log Success:';
PRINT '   EXEC dbo.usp_LogPipelineSuccess @execution_id = ''@{pipeline().RunId}'', @job_name = ''@{pipeline().PipelineName}'';';
PRINT '';
PRINT '   -- Log Failure:';
PRINT '   EXEC dbo.usp_LogPipelineFailure @execution_id = ''@{pipeline().RunId}'', @job_name = ''@{pipeline().PipelineName}'', @error_message = ''@{activity(''YourActivity'').error.message}'';';
GO
