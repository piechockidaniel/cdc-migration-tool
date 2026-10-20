-- run after the container is healthy:
--   docker exec -i <mssql> /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P 'CdcSync!2024' -i /init/mssql.sql
CREATE DATABASE source;
GO
CREATE DATABASE target;
GO
USE source;
GO
ALTER DATABASE source SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE;
EXEC sys.sp_cdc_enable_db;
GO
CREATE TABLE dbo.Customers (CustomerID INT NOT NULL PRIMARY KEY, Name NVARCHAR(100) NOT NULL, Email NVARCHAR(200) NULL,
                            City NVARCHAR(100) NULL, ModifiedDate DATETIME2 NOT NULL DEFAULT SYSDATETIME());
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'Customers', @role_name = NULL, @capture_instance = N'dbo_Customers';
GO
USE target;
GO
CREATE TABLE dbo.customers (id INT NOT NULL PRIMARY KEY, name NVARCHAR(100) NOT NULL, email NVARCHAR(200) NULL, city NVARCHAR(100) NULL);
GO
