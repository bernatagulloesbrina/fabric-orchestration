CREATE TABLE [dbo].[jobs] (
    [execution_id]  NVARCHAR (50)  NULL,
    [job_name]      NVARCHAR (200) NOT NULL,
    [start_time]    DATETIME2 (7)  NULL,
    [end_time]      DATETIME2 (7)  NULL,
    [result]        NVARCHAR (50)  NULL,
    [error_message] NVARCHAR (MAX) NULL,
    [created_at]    DATETIME2 (7)  NULL,
    [job_type]      NVARCHAR (50)  NULL,
    [object_type]   NVARCHAR (50)  NULL,
    CONSTRAINT [PK_jobs] PRIMARY KEY CLUSTERED ([job_name] ASC)
);


GO

