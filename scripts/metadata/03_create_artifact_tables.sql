-- ============================================================
-- Target : Metadata (Fabric SQL Database)
-- Purpose: Fabric tenant artifact snapshot.
--          Fully truncated and reloaded on every run of
--          the "Load Fabric Items" pipeline.
-- ============================================================

CREATE TABLE dbo.workspaces (
    workspace_id  NVARCHAR(100)  NULL,
    display_name  NVARCHAR(500)  NULL,
    type          NVARCHAR(100)  NULL,
    state         NVARCHAR(50)   NULL,
    harvested_at  DATETIME2(7)   NULL
);

CREATE TABLE dbo.semantic_models (
    workspace_id  NVARCHAR(100)  NULL,
    item_id       NVARCHAR(100)  NULL,
    display_name  NVARCHAR(500)  NULL,
    description   NVARCHAR(MAX)  NULL,
    harvested_at  DATETIME2(7)   NULL
);

CREATE TABLE dbo.dataflows (
    workspace_id  NVARCHAR(100)  NULL,
    item_id       NVARCHAR(100)  NULL,
    display_name  NVARCHAR(500)  NULL,
    harvested_at  DATETIME2(7)   NULL
);

CREATE TABLE dbo.pipelines (
    workspace_id  NVARCHAR(100)  NULL,
    item_id       NVARCHAR(100)  NULL,
    display_name  NVARCHAR(500)  NULL,
    harvested_at  DATETIME2(7)   NULL
);
