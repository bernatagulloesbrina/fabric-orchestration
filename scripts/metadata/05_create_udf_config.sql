-- ============================================================
-- Target : Metadata (Fabric SQL Database)
-- Purpose: Configuration table for Fabric User Data Functions.
--          Stores non-code secrets (e.g. service principal
--          credentials) that UDFs read at runtime.
--          Values are protected by Fabric workspace access
--          control and never committed to the repository.
-- ============================================================

CREATE TABLE dbo.udf_config (
    config_key   NVARCHAR(200) NOT NULL,
    config_value NVARCHAR(MAX) NOT NULL,
    CONSTRAINT PK_udf_config PRIMARY KEY (config_key)
);

-- Service principal credentials used by triggerOnDemandRefresh UDF.
-- Replace placeholder values with real values before running.
-- The SP must be added as Contributor to the Fabric workspace and
-- "Service principals can use Fabric APIs" must be enabled in the Fabric Admin Portal.
INSERT INTO dbo.udf_config (config_key, config_value) VALUES ('SP_TENANT_ID',     '<your-tenant-id>');
INSERT INTO dbo.udf_config (config_key, config_value) VALUES ('SP_CLIENT_ID',     '<your-client-id>');
INSERT INTO dbo.udf_config (config_key, config_value) VALUES ('SP_CLIENT_SECRET', '<your-client-secret>');

-- Name of the Fabric workspace where pipeline "09_Refresh Fabric Item On Demand" is deployed.
-- The Fabric REST API has no cross-workspace search, so the UDF must know which workspace
-- to look in before it can find the pipeline by name.
-- Use the exact display name shown in the top-left of the Fabric portal (e.g. 'My Fabric Workspace').
INSERT INTO dbo.udf_config (config_key, config_value) VALUES ('PIPELINE_WORKSPACE_NAME', '<your-workspace-display-name>');
