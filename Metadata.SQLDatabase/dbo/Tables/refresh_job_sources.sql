-- Local copy of the lakehouse table refresh_job_sources, reloaded on every run of the
-- "Load Fabric Items" pipeline (a Copy activity: lakehouse refresh_job_sources -> this table).
-- Required because the transactional Fabric SQL Database engine cannot cross-query the lakehouse
-- (Msg 40515: three-part / cross-database names are not supported). The refresh_jobs_ready view
-- reads SharePoint dependencies from this local copy.
--
-- No primary key: this is a reload-only sink for a Copy activity, and refresh_jobs_ready reads it
-- with SELECT DISTINCT, so duplicate rows are harmless. A PK only let the Copy hard-fail on any
-- duplicate (object_id, datasource_id) in the source. (Side effect: the table is not mirrored to
-- OneLake, which it doesn't need to be.)
CREATE TABLE [dbo].[refresh_job_sources] (
    [job_name]              NVARCHAR (200) NULL,
    [object_id]             NVARCHAR (100) NULL,
    [object_type]           NVARCHAR (100) NULL,
    [datasource_type]       NVARCHAR (200) NULL,
    [datasource_connection] NVARCHAR (MAX) NULL,
    [datasource_id]         NVARCHAR (100) NULL
);


GO
