-- ============================================================
-- Target : Metadata (Fabric SQL Database)
-- Purpose: Stores refreshable Fabric items and their precedence graph.
-- Note   : Primary keys required for DML operations in Fabric SQL DB
-- ============================================================

CREATE TABLE dbo.refresh_jobs (
    job_name       NVARCHAR(200)  NOT NULL PRIMARY KEY,
    workspace_id   NVARCHAR(100)  NOT NULL,
    workspace_name NVARCHAR(500)  NOT NULL,
    object_type    NVARCHAR(100)  NOT NULL,
    object_id      NVARCHAR(100)  NOT NULL,
    object_name    NVARCHAR(500)  NOT NULL,
    priority       INT            NOT NULL
);

CREATE TABLE dbo.refresh_job_precedence (
    job_name           NVARCHAR(200) NOT NULL,
    precedent_job_name NVARCHAR(200) NOT NULL,
    PRIMARY KEY (job_name, precedent_job_name)
);
