CREATE TABLE [dbo].[workspaces] (
    [id]          NVARCHAR(100) NOT NULL,
    [displayName] NVARCHAR(255) NOT NULL,
    [capacityId]  NVARCHAR(100) NULL,
    [type]        NVARCHAR(50)  NULL,
    [loaded_at]   DATETIME2     NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_workspaces] PRIMARY KEY ([id])
);
