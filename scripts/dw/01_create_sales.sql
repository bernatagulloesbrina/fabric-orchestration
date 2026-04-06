-- ============================================================
-- Target : DW (Fabric Warehouse)
-- Purpose: Destination for dummy sales data.
--
-- Fabric Warehouse constraints applied:
--   - No IDENTITY columns
--   - No enforced PRIMARY KEY / FOREIGN KEY
--   - DATETIME2(n) required (not plain DATETIME)
--   - sale_id generated with NEWID() at insert time
-- ============================================================

CREATE TABLE dbo.Sales (
    sale_id    VARCHAR(50)     NULL,
    sale_date  DATETIME2(0)    NULL,   -- date only precision
    amount     DECIMAL(10, 2)  NULL,
    created_at DATETIME2(6)    NULL
);
