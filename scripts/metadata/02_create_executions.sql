-- ============================================================
-- Target : Metadata (Fabric SQL Database)
-- Purpose: Append-only log of every pipeline execution.
--          One new row is inserted per pipeline run.
-- ============================================================

CREATE TABLE dbo.executions (
    execution_id   NVARCHAR(50)   NOT NULL,   -- pipeline run ID (passed from ADF/Fabric system variable)
    job_name       NVARCHAR(200)  NOT NULL,
    start_time     DATETIME2(7)   NULL,
    end_time       DATETIME2(7)   NULL,
    result         NVARCHAR(50)   NULL,        -- 'Success' | 'Failed'
    error_message  NVARCHAR(MAX)  NULL,
    created_at     DATETIME2(7)   NULL
);
