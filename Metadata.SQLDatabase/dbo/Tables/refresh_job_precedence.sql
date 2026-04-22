CREATE TABLE [dbo].[refresh_job_precedence] (
    [job_name]           NVARCHAR (200) NOT NULL,
    [precedent_job_name] NVARCHAR (200) NOT NULL,
    CONSTRAINT [PK_refresh_job_precedence] PRIMARY KEY CLUSTERED ([job_name] ASC, [precedent_job_name] ASC)
);


GO

