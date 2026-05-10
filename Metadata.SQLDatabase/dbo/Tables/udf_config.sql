CREATE TABLE [dbo].[udf_config] (
    [config_key]   NVARCHAR (200) NOT NULL,
    [config_value] NVARCHAR (MAX) NOT NULL,
    CONSTRAINT [PK_udf_config] PRIMARY KEY CLUSTERED ([config_key] ASC)
);


GO

