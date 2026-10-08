CREATE USER [adf-olist-dev-bd] FROM EXTERNAL PROVIDER;
CREATE USER [id-olist-deploy-dev] FROM EXTERNAL PROVIDER;
ALTER ROLE db_owner ADD MEMBER [id-olist-deploy-dev];
SELECT name, type_desc, create_date FROM sys.database_principals WHERE type IN ('E', 'X') ORDER BY name;
