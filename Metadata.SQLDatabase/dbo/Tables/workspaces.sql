CREATE TABLE [dbo].[workspaces] (
    [workspace_id] NVARCHAR (100) NULL,
    [display_name] NVARCHAR (500) NULL,
    [type]         NVARCHAR (100) NULL,
    [state]        NVARCHAR (50)  NULL,
    [harvested_at] DATETIME2 (7)  NULL
);


GO

