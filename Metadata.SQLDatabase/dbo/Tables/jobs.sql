CREATE TABLE [dbo].[jobs] (
    [job_name] NVARCHAR(255) NOT NULL,
    [start]    DATETIME2(6)  NOT NULL,
    [end]      DATETIME2(6)  NULL,
    [result]   NVARCHAR(50)  NULL,
    CONSTRAINT [PK_jobs] PRIMARY KEY ([job_name])
);
