CREATE TABLE [dbo].[refreshes] (
    [job_name]       NVARCHAR(255) NOT NULL,
    [object_type]    NVARCHAR(50)  NOT NULL,
    [workspace_id]   NVARCHAR(100) NOT NULL,
    [workspace_name] NVARCHAR(255) NOT NULL,
    [object_id]      NVARCHAR(100) NOT NULL,
    [object_name]    NVARCHAR(255) NOT NULL,
    CONSTRAINT [PK_refreshes] PRIMARY KEY ([job_name], [object_id])
);
