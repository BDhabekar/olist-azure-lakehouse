IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'adf-olist-prod-bd') CREATE USER [adf-olist-prod-bd] FROM EXTERNAL PROVIDER;
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'id-olist-deploy-prod') CREATE USER [id-olist-deploy-prod] FROM EXTERNAL PROVIDER;
ALTER ROLE db_owner ADD MEMBER [id-olist-deploy-prod];
SELECT name, type_desc, create_date FROM sys.database_principals WHERE type IN ('E', 'X') ORDER BY name;
