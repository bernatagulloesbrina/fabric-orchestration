CREATE TABLE [dbo].[Sales] (
    [SaleId]    INT            IDENTITY(1, 1) NOT NULL,
    [SaleDate]  DATE           NOT NULL,
    [Amount]    DECIMAL(10, 2) NOT NULL,
    [CreatedAt] DATETIME2      NOT NULL DEFAULT GETDATE(),
    CONSTRAINT [PK_Sales] PRIMARY KEY ([SaleId])
);
