CREATE TABLE [dbo].[dataflows] (
    [workspace_id] NVARCHAR(100) NOT NULL,
    [id]           NVARCHAR(100) NOT NULL,
    [displayName]  NVARCHAR(255) NOT NULL,
    [type]         NVARCHAR(50)  NULL,
    [loaded_at]    DATETIME2(6)  NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_dataflows] PRIMARY KEY ([workspace_id], [id])
);
