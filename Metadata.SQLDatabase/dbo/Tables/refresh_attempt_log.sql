CREATE TABLE [dbo].[refresh_attempt_log] (
    [attempt_log_id] BIGINT IDENTITY(1,1) NOT NULL,
    [logged_at] DATETIME2(7) NOT NULL CONSTRAINT [DF_refresh_attempt_log_logged_at] DEFAULT (GETUTCDATE()),
    [pipeline_run_id] NVARCHAR(100) NOT NULL,
    [job_name] NVARCHAR(200) NOT NULL,
    [workspace_id] NVARCHAR(100) NULL,
    [object_id] NVARCHAR(100) NULL,
    [object_type] NVARCHAR(50) NULL,
    [phase] NVARCHAR(50) NOT NULL,
    [item_status] NVARCHAR(50) NULL,
    [http_status] NVARCHAR(20) NULL,
    [api_response_json] NVARCHAR(MAX) NULL,
    [error_message] NVARCHAR(MAX) NULL,
    CONSTRAINT [PK_refresh_attempt_log] PRIMARY KEY CLUSTERED ([attempt_log_id] ASC)
);

GO
