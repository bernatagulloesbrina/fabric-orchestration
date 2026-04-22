-- ============================================================
-- Target : Metadata (Fabric SQL Database)
-- Purpose: One row per job; reflects the LATEST execution only.
--          Upserted by every pipeline run (MERGE on job_name).
-- Note   : Primary key required for DML operations in Fabric SQL DB
-- ============================================================

CREATE TABLE dbo.jobs (
    job_name        NVARCHAR(200)  NOT NULL PRIMARY KEY,   -- logical job name
    last_start_time DATETIME2(7)   NULL,
    last_end_time   DATETIME2(7)   NULL,
    last_result     NVARCHAR(50)   NULL,                   -- 'Success' | 'Failed'
    error_message   NVARCHAR(MAX)  NULL,
    updated_at      DATETIME2(7)   NULL,
    job_type        NVARCHAR(50)   NULL,
    object_type     NVARCHAR(100)  NULL
);
