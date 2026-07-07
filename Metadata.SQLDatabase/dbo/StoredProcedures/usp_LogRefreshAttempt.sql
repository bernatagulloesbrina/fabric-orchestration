CREATE PROCEDURE [dbo].[usp_LogRefreshAttempt]
    @pipeline_run_id NVARCHAR(100),
    @job_name NVARCHAR(200),
    @workspace_id NVARCHAR(100) = NULL,
    @object_id NVARCHAR(100) = NULL,
    @object_type NVARCHAR(50) = NULL,
    @phase NVARCHAR(50),
    @item_status NVARCHAR(50) = NULL,
    @http_status NVARCHAR(20) = NULL,
    @api_response_json NVARCHAR(MAX) = NULL,
    @error_message NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO dbo.refresh_attempt_log (
        pipeline_run_id,
        job_name,
        workspace_id,
        object_id,
        object_type,
        phase,
        item_status,
        http_status,
        api_response_json,
        error_message
    )
    VALUES (
        @pipeline_run_id,
        @job_name,
        @workspace_id,
        @object_id,
        @object_type,
        @phase,
        @item_status,
        @http_status,
        @api_response_json,
        @error_message
    );
END

GO
