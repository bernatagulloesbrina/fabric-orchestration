CREATE TABLE [dbo].[executions] (
    [execution_id]  NVARCHAR (50)  NOT NULL,
    [job_name]      NVARCHAR (200) NOT NULL,
    [start_time]    DATETIME2 (7)  NULL,
    [end_time]      DATETIME2 (7)  NULL,
    [result]        NVARCHAR (50)  NULL,
    [error_message] NVARCHAR (MAX) NULL,
    [created_at]    DATETIME2 (7)  NULL
);


GO

