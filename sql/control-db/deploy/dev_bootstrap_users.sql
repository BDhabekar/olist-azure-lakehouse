IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'adf-olist-dev-bd') CREATE USER [adf-olist-dev-bd] FROM EXTERNAL PROVIDER;
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'id-olist-deploy-dev') CREATE USER [id-olist-deploy-dev] FROM EXTERNAL PROVIDER;
ALTER ROLE db_owner ADD MEMBER [id-olist-deploy-dev];
SELECT name, type_desc, create_date FROM sys.database_principals WHERE type IN ('E', 'X') ORDER BY name;
