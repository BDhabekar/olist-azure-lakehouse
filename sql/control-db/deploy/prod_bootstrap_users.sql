CREATE USER [adf-olist-prod-bd] FROM EXTERNAL PROVIDER;
CREATE USER [id-olist-deploy] FROM EXTERNAL PROVIDER;
ALTER ROLE db_owner ADD MEMBER [id-olist-deploy];
SELECT name, type_desc, create_date FROM sys.database_principals WHERE type IN ('E', 'X') ORDER BY name;
