-- FinanceHub — Azure SQL star schema (T-SQL)
-- Run in Azure SQL Database, Azure SQL MI, or Synapse dedicated pool (adjust types if needed).

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_date')
BEGIN
    CREATE TABLE dim_date (
        date_key INT NOT NULL PRIMARY KEY,
        full_date DATE NULL,
        day_name NVARCHAR(20) NULL,
        month_name NVARCHAR(20) NULL,
        month_num INT NULL,
        quarter_num INT NULL,
        year_num INT NULL,
        is_weekend BIT NULL
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_customer')
BEGIN
    CREATE TABLE dim_customer (
        customer_key NVARCHAR(50) NOT NULL PRIMARY KEY,
        country NVARCHAR(100) NULL,
        rfm_segment NVARCHAR(50) NULL,
        rfm_score INT NULL,
        valid_from DATETIME2 NULL,
        valid_to DATETIME2 NULL,
        is_current BIT NULL
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'dim_product')
BEGIN
    CREATE TABLE dim_product (
        product_key NVARCHAR(50) NOT NULL PRIMARY KEY,
        description NVARCHAR(255) NULL,
        unit_price_gbp FLOAT NULL
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'fact_sales')
BEGIN
    CREATE TABLE fact_sales (
        sales_id INT IDENTITY(1,1) PRIMARY KEY,
        invoice_no NVARCHAR(50) NULL,
        invoice_date DATETIME2 NULL,
        customer_key NVARCHAR(50) NULL,
        product_key NVARCHAR(50) NULL,
        quantity INT NULL,
        unit_price FLOAT NULL,
        total_gbp FLOAT NULL,
        total_usd FLOAT NULL,
        total_eur FLOAT NULL,
        total_mad FLOAT NULL,
        is_fraud_suspect BIT NULL,
        is_deep_fraud_suspect BIT NULL,
        fraud_probability FLOAT NULL
    );
    CREATE INDEX IX_fact_sales_invoice_date ON fact_sales (invoice_date);
END
GO