-- SQL Script to set up test databases for MSSQL CDC Synchronizer
-- Run this script on your SQL Server instance to create test databases

-- ============================================================================
-- PART 1: Create Source Database
-- ============================================================================

USE master;
GO

-- Drop if exists
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'CDCSourceDB')
BEGIN
    ALTER DATABASE CDCSourceDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE CDCSourceDB;
END
GO

-- Create source database
CREATE DATABASE CDCSourceDB;
GO

USE CDCSourceDB;
GO

-- Create a test table
CREATE TABLE dbo.Customers (
    CustomerID INT PRIMARY KEY IDENTITY(1,1),
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100),
    CreatedDate DATETIME DEFAULT GETDATE(),
    ModifiedDate DATETIME DEFAULT GETDATE()
);
GO

-- Insert sample data
INSERT INTO dbo.Customers (CustomerName, Email)
VALUES 
    ('John Doe', 'john.doe@example.com'),
    ('Jane Smith', 'jane.smith@example.com'),
    ('Bob Johnson', 'bob.johnson@example.com'),
    ('Alice Williams', 'alice.williams@example.com'),
    ('Charlie Brown', 'charlie.brown@example.com');
GO

-- Create another test table
CREATE TABLE dbo.Orders (
    OrderID INT PRIMARY KEY IDENTITY(1,1),
    CustomerID INT FOREIGN KEY REFERENCES dbo.Customers(CustomerID),
    OrderDate DATETIME DEFAULT GETDATE(),
    TotalAmount DECIMAL(10,2),
    Status NVARCHAR(20)
);
GO

-- Insert sample orders
INSERT INTO dbo.Orders (CustomerID, OrderDate, TotalAmount, Status)
VALUES 
    (1, GETDATE(), 150.00, 'Completed'),
    (2, GETDATE(), 250.50, 'Pending'),
    (3, GETDATE(), 75.25, 'Completed'),
    (1, GETDATE(), 300.00, 'Shipped'),
    (4, GETDATE(), 125.75, 'Pending');
GO

-- Enable CDC on database
EXEC sys.sp_cdc_enable_db;
GO

-- Enable CDC on Customers table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Customers',
    @role_name = NULL,
    @supports_net_changes = 1;
GO

-- Enable CDC on Orders table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Orders',
    @role_name = NULL,
    @supports_net_changes = 1;
GO

-- Verify CDC is enabled
SELECT 
    name AS DatabaseName,
    is_cdc_enabled AS CDCEnabled
FROM sys.databases
WHERE name = 'CDCSourceDB';
GO

-- List CDC-enabled tables
SELECT 
    s.name AS SchemaName,
    t.name AS TableName,
    ct.capture_instance AS CaptureInstance,
    ct.start_lsn AS StartLSN
FROM cdc.change_tables ct
JOIN sys.tables t ON ct.source_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id;
GO

-- ============================================================================
-- PART 2: Create Destination Database
-- ============================================================================

USE master;
GO

-- Drop if exists
IF EXISTS (SELECT * FROM sys.databases WHERE name = 'CDCDestDB')
BEGIN
    ALTER DATABASE CDCDestDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE CDCDestDB;
END
GO

-- Create destination database
CREATE DATABASE CDCDestDB;
GO

USE CDCDestDB;
GO

-- Create matching tables (CDC not needed on destination)
CREATE TABLE dbo.Customers (
    CustomerID INT PRIMARY KEY,
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100),
    CreatedDate DATETIME,
    ModifiedDate DATETIME
);
GO

CREATE TABLE dbo.Orders (
    OrderID INT PRIMARY KEY,
    CustomerID INT,
    OrderDate DATETIME,
    TotalAmount DECIMAL(10,2),
    Status NVARCHAR(20)
);
GO

-- Note: Destination tables start empty - they will be populated by synchronization

-- ============================================================================
-- PART 3: Verification Queries
-- ============================================================================

USE CDCSourceDB;
GO

PRINT 'Source Database - Customers Count:';
SELECT COUNT(*) AS CustomerCount FROM dbo.Customers;
GO

PRINT 'Source Database - Orders Count:';
SELECT COUNT(*) AS OrderCount FROM dbo.Orders;
GO

USE CDCDestDB;
GO

PRINT 'Destination Database - Customers Count (should be 0 initially):';
SELECT COUNT(*) AS CustomerCount FROM dbo.Customers;
GO

PRINT 'Destination Database - Orders Count (should be 0 initially):';
SELECT COUNT(*) AS OrderCount FROM dbo.Orders;
GO

-- ============================================================================
-- PART 4: Test Data Modification Procedures (for testing CDC)
-- ============================================================================

USE CDCSourceDB;
GO

-- Procedure to generate test INSERT operations
CREATE OR ALTER PROCEDURE dbo.GenerateTestInserts
    @Count INT = 10
AS
BEGIN
    DECLARE @i INT = 1;
    
    WHILE @i <= @Count
    BEGIN
        INSERT INTO dbo.Customers (CustomerName, Email)
        VALUES (
            'Test Customer ' + CAST(@i AS NVARCHAR(10)),
            'test' + CAST(@i AS NVARCHAR(10)) + '@example.com'
        );
        
        SET @i = @i + 1;
        WAITFOR DELAY '00:00:01'; -- 1 second delay between inserts
    END
    
    PRINT CAST(@Count AS NVARCHAR(10)) + ' test customers inserted';
END
GO

-- Procedure to generate test UPDATE operations
CREATE OR ALTER PROCEDURE dbo.GenerateTestUpdates
    @Count INT = 5
AS
BEGIN
    DECLARE @i INT = 1;
    
    WHILE @i <= @Count
    BEGIN
        UPDATE dbo.Customers
        SET 
            Email = 'updated' + CAST(@i AS NVARCHAR(10)) + '@example.com',
            ModifiedDate = GETDATE()
        WHERE CustomerID = @i;
        
        SET @i = @i + 1;
        WAITFOR DELAY '00:00:01'; -- 1 second delay between updates
    END
    
    PRINT CAST(@Count AS NVARCHAR(10)) + ' customers updated';
END
GO

-- Procedure to generate test DELETE operations
CREATE OR ALTER PROCEDURE dbo.GenerateTestDeletes
    @StartID INT = 100,
    @Count INT = 5
AS
BEGIN
    DECLARE @i INT = 0;
    
    WHILE @i < @Count
    BEGIN
        DELETE FROM dbo.Orders WHERE CustomerID = @StartID + @i;
        DELETE FROM dbo.Customers WHERE CustomerID = @StartID + @i;
        
        SET @i = @i + 1;
        WAITFOR DELAY '00:00:01'; -- 1 second delay between deletes
    END
    
    PRINT CAST(@Count AS NVARCHAR(10)) + ' customers deleted';
END
GO

-- ============================================================================
-- USAGE INSTRUCTIONS
-- ============================================================================

/*

After running this script:

1. Source Database (CDCSourceDB) will be created with:
   - CDC enabled
   - Two tables: Customers and Orders
   - Sample data already inserted
   - Helper stored procedures for testing

2. Destination Database (CDCDestDB) will be created with:
   - Same table structure as source
   - Empty tables (ready to receive synchronized data)

3. To test CDC synchronization:
   
   -- Generate test inserts:
   USE CDCSourceDB;
   EXEC dbo.GenerateTestInserts @Count = 10;
   
   -- Generate test updates:
   EXEC dbo.GenerateTestUpdates @Count = 5;
   
   -- Generate test deletes (use high IDs to avoid FK conflicts):
   EXEC dbo.GenerateTestDeletes @StartID = 100, @Count = 5;

4. Configure the synchronizer application to use:
   - Source: localhost / CDCSourceDB
   - Destination: localhost / CDCDestDB

5. Monitor synchronization in the web UI

6. Compare data:
   SELECT * FROM CDCSourceDB.dbo.Customers;
   SELECT * FROM CDCDestDB.dbo.Customers;

*/

PRINT '';
PRINT '============================================================';
PRINT 'Test databases created successfully!';
PRINT '============================================================';
PRINT 'Source Database: CDCSourceDB (CDC enabled)';
PRINT 'Destination Database: CDCDestDB (empty, ready for sync)';
PRINT '';
PRINT 'Next steps:';
PRINT '1. Configure application connections';
PRINT '2. Create table mappings';
PRINT '3. Start synchronization';
PRINT '4. Run test procedures to generate CDC events';
PRINT '============================================================';
GO








