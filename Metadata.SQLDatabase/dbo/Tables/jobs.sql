CREATE TABLE [dbo].[jobs] (
    [job_name]        NVARCHAR (200) NOT NULL,
    [last_start_time] DATETIME2 (7)  NULL,
    [last_end_time]   DATETIME2 (7)  NULL,
    [last_result]     NVARCHAR (50)  NULL,
    [error_message]   NVARCHAR (MAX) NULL,
    [updated_at]      DATETIME2 (7)  NULL
);


GO

