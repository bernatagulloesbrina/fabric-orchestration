CREATE TABLE [dbo].[semantic_models] (
    [workspace_id] NVARCHAR (100) NULL,
    [item_id]      NVARCHAR (100) NULL,
    [display_name] NVARCHAR (500) NULL,
    [description]  NVARCHAR (MAX) NULL,
    [harvested_at] DATETIME2 (7)  NULL
);


GO

