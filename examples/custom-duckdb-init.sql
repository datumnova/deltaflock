-- Example custom DuckDB initialization file
-- This demonstrates how you can provide your own initialization SQL at startup
-- Copy this file and modify it according to your needs, then set duckdb_init_sql_file
-- environment variable to point to your custom file

-- Load required extensions
INSTALL uc_catalog;
INSTALL delta;
LOAD delta;
LOAD uc_catalog;

-- INSTALL azure;
-- INSTALL delta;
-- LOAD delta;
-- LOAD azure;
-- set azure_transport_option_type = 'curl';

-- Create Unity Catalog secret for your environment
-- Customize the endpoint and use environment variable substitution
CREATE SECRET (
    TYPE UC,
    TOKEN '${UC_CATALOG_TOKEN}',
    ENDPOINT '${UC_CATALOG_ENDPOINT}'
);

-- Example: Different Azure authentication method
-- Uncomment and modify based on your Azure setup
-- CREATE SECRET (
--     TYPE azure,
--     PROVIDER service_principal,
--     TENANT_ID '${AZURE_TENANT_ID}',
--     CLIENT_ID '${AZURE_CLIENT_ID}',
--     CLIENT_SECRET '${AZURE_CLIENT_SECRET}',
--     ACCOUNT_NAME '${AZURE_STORAGE_ACCOUNT_NAME}',
--     SCOPE 'azure://${AZURE_STORAGE_ACCOUNT_NAME}'
-- );

-- Attach different catalogs based on your environment
-- ATTACH 'production' AS prod (TYPE UC_CATALOG);
-- ATTACH 'staging' AS staging (TYPE UC_CATALOG);

-- Set up additional configurations
-- SET memory_limit = '8GB';
-- SET threads = 4;

-- Custom functions or views can also be defined here
-- CREATE VIEW my_view AS SELECT * FROM some_table WHERE condition = 'value';
