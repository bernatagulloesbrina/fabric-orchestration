-- ============================================================
-- Target : Metadata (Fabric SQL Database)
-- Purpose: Fabric tenant artifact snapshot.
--          Fully cleared and reloaded on every run of
--          the "Load Fabric Items" pipeline.
-- Note   : Primary keys required for DML operations in Fabric SQL DB
-- ============================================================

CREATE TABLE dbo.workspaces (
    workspace_id  NVARCHAR(100)  NOT NULL PRIMARY KEY,
    display_name  NVARCHAR(500)  NULL,
    type          NVARCHAR(100)  NULL,
    state         NVARCHAR(50)   NULL,
    harvested_at  DATETIME2(7)   NULL
);

CREATE TABLE dbo.semantic_models (
    workspace_id  NVARCHAR(100)  NOT NULL,
    item_id       NVARCHAR(100)  NOT NULL,
    display_name  NVARCHAR(500)  NULL,
    description   NVARCHAR(MAX)  NULL,
    harvested_at  DATETIME2(7)   NULL,
    PRIMARY KEY (workspace_id, item_id)
);

CREATE TABLE dbo.dataflows (
    workspace_id  NVARCHAR(100)  NOT NULL,
    item_id       NVARCHAR(100)  NOT NULL,
    display_name  NVARCHAR(500)  NULL,
    harvested_at  DATETIME2(7)   NULL,
    PRIMARY KEY (workspace_id, item_id)
);

CREATE TABLE dbo.pipelines (
    workspace_id  NVARCHAR(100)  NOT NULL,
    item_id       NVARCHAR(100)  NOT NULL,
    display_name  NVARCHAR(500)  NULL,
    harvested_at  DATETIME2(7)   NULL,
    PRIMARY KEY (workspace_id, item_id)
);
