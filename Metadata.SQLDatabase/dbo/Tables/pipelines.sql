CREATE TABLE [dbo].[pipelines] (
    [workspace_id] NVARCHAR(100) NOT NULL,
    [id]           NVARCHAR(100) NOT NULL,
    [displayName]  NVARCHAR(255) NOT NULL,
    [type]         NVARCHAR(50)  NULL,
    [loaded_at]    DATETIME2     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_pipelines] PRIMARY KEY ([workspace_id], [id])
);
