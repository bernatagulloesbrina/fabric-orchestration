CREATE TABLE [dbo].[refresh_precedents] (
    [job_name]           NVARCHAR(255) NOT NULL,
    [precedent_job_name] NVARCHAR(255) NOT NULL,
    CONSTRAINT [PK_refresh_precedents] PRIMARY KEY ([job_name], [precedent_job_name])
);
