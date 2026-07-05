CREATE TABLE [dbo].[refresh_job_sources] (
    [job_name]              NVARCHAR (200) NULL,
    [object_id]             NVARCHAR (100) NULL,
    [object_type]           NVARCHAR (100) NULL,
    [datasource_type]       NVARCHAR (200) NULL,
    [datasource_connection] NVARCHAR (MAX) NULL,
    [datasource_id]         NVARCHAR (100) NULL
);


GO

