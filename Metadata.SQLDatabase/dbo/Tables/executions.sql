CREATE TABLE [dbo].[executions] (
    [id]             INT           IDENTITY(1, 1) NOT NULL,
    [execution_date] DATETIME2(6)  NOT NULL,
    [job_name]       NVARCHAR(255) NOT NULL,
    [start]          DATETIME2(6)  NOT NULL,
    [end]            DATETIME2(6)  NULL,
    [result]         NVARCHAR(50)  NULL,
    CONSTRAINT [PK_executions] PRIMARY KEY ([id])
);
