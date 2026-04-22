CREATE TABLE [dbo].[refresh_jobs] (
    [job_name]       NVARCHAR (200) NOT NULL,
    [workspace_id]   NVARCHAR (100) NOT NULL,
    [workspace_name] NVARCHAR (500) NOT NULL,
    [object_type]    NVARCHAR (100) NOT NULL,
    [object_id]      NVARCHAR (100) NOT NULL,
    [object_name]    NVARCHAR (500) NOT NULL,
    [priority]       INT            NOT NULL,
    CONSTRAINT [PK_refresh_jobs] PRIMARY KEY CLUSTERED ([job_name] ASC)
);


GO

