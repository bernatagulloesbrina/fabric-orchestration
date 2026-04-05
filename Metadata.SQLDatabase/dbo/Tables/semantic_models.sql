CREATE TABLE [dbo].[semantic_models] (
    [workspace_id]  NVARCHAR(100) NOT NULL,
    [id]            NVARCHAR(100) NOT NULL,
    [displayName]   NVARCHAR(255) NOT NULL,
    [isRefreshable] BIT           NULL,
    [loaded_at]     DATETIME2(6)  NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_semantic_models] PRIMARY KEY ([workspace_id], [id])
);
