-- Local copy of the lakehouse table refresh_job_sources, reloaded on every run of the
-- "Load Fabric Items" pipeline (a Copy activity: lakehouse refresh_job_sources -> this table).
-- Required because the transactional Fabric SQL Database engine cannot cross-query the lakehouse
-- (Msg 40515: three-part / cross-database names are not supported). The refresh_jobs_ready view
-- reads SharePoint dependencies from this local copy.
-- Grain (and primary key): one row per (object_id, datasource_id) -- matches the notebook de-dup.
CREATE TABLE [dbo].[refresh_job_sources] (
    [job_name]              NVARCHAR (200) NULL,
    [object_id]             NVARCHAR (100) NOT NULL,
    [object_type]           NVARCHAR (100) NULL,
    [datasource_type]       NVARCHAR (200) NULL,
    [datasource_connection] NVARCHAR (MAX) NULL,
    [datasource_id]         NVARCHAR (100) NOT NULL,
    CONSTRAINT [PK_refresh_job_sources] PRIMARY KEY CLUSTERED ([object_id] ASC, [datasource_id] ASC)
);


GO
