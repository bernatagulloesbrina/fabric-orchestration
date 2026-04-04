CREATE TABLE [dbo].[refresh_logs] (
    [LogId]         INT           IDENTITY(1, 1) NOT NULL,
    [pipeline_name] NVARCHAR(255) NOT NULL,
    [start_time]    DATETIME2     NOT NULL,
    [end_time]      DATETIME2     NOT NULL,
    [result]        NVARCHAR(50)  NOT NULL,
    CONSTRAINT [PK_refresh_logs] PRIMARY KEY ([LogId])
);
