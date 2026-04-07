-- ============================================================
-- Target : DW (Fabric Warehouse)
-- Source : sales_staging table in StagingLakehouse
-- Purpose: Copy staged sales data from lakehouse to warehouse
--
-- This script should run AFTER LoadSalesData notebook
-- Note  : Replace [workspace-id] and [lakehouse-id] with actual values
--         from your StagingLakehouse in Fabric
-- ============================================================

-- Insert sales data from lakehouse staging to warehouse
-- Using NEWID() to generate unique sale_id values
INSERT INTO dbo.Sales (sale_id, sale_date, amount, created_at)
SELECT 
    NEWID() AS sale_id,
    CAST(sale_date AS DATETIME2(0)) AS sale_date,
    CAST(amount AS DECIMAL(10, 2)) AS amount,
    created_at
FROM OPENROWSET(
    BULK 'https://onelake.dfs.fabric.microsoft.com/[workspace-id]/[lakehouse-id]/Tables/sales_staging',
    FORMAT = 'DELTA'
) AS staging;

-- Alternative if cross-database query is configured:
-- INSERT INTO dbo.Sales (sale_id, sale_date, amount, created_at)
-- SELECT NEWID(), sale_date, amount, created_at
-- FROM [LakehouseName].dbo.sales_staging;
