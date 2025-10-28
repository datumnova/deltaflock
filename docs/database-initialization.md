# Database Initialization Configuration

This document explains how to configure custom DuckDB initialization SQL at startup instead of using the bundled initialization code.

## Configuration Options

The application supports two methods for providing database initialization SQL, in order of priority:

### 1. Custom SQL File

Set the `duckdb_init_sql_file` environment variable to point to a custom SQL file:

```bash
export duckdb_init_sql_file=/path/to/your/custom-init.sql
```

Or in your `.env` file:
```bash
duckdb_init_sql_file=/path/to/your/custom-init.sql
```

### 2. Bundled Default (Fallback)

If no custom file is provided, the application will use the bundled `src/app/db/duckdb_init.sql` file.

## Environment Variable Substitution

All initialization SQL (regardless of source) supports environment variable substitution using the `${VARIABLE_NAME}` syntax. The following variables are automatically available:

- `UC_CATALOG_ENDPOINT`
- `UC_CATALOG_TOKEN`
- `AZURE_STORAGE_ACCOUNT_NAME`
- `AZURE_STORAGE_ACCOUNT_KEY`
- `AZURE_ACCOUNT_NAME`
- `AZURE_CLIENT_ID`
- `AZURE_CLIENT_SECRET`
- `AZURE_TENANT_ID`

## Example Custom Initialization File

See `examples/custom-duckdb-init.sql` for a complete example of how to structure your custom initialization SQL.

## Use Cases

### Development vs Production

Use different initialization SQL for different environments:

**Development (.env.dev):**
```bash
duckdb_init_sql_file=./config/dev-duckdb-init.sql
```

**Production (.env.prod):**
```bash
duckdb_init_sql_file=./config/prod-duckdb-init.sql
```

### Docker Deployment

Mount your custom initialization file and configure the path:

```dockerfile
# In your Dockerfile
COPY custom-init.sql /app/config/init.sql

# Set environment variable
ENV duckdb_init_sql_file=/app/config/init.sql
```

### Kubernetes ConfigMap

Create a ConfigMap with your initialization SQL:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: duckdb-init-config
data:
  init.sql: |
    INSTALL delta;
    LOAD delta;
    CREATE SECRET (TYPE UC, TOKEN '${UC_CATALOG_TOKEN}', ENDPOINT '${UC_CATALOG_ENDPOINT}');
    -- Your custom initialization here
```

Then mount it and configure the path in your deployment.

### Dynamic File Generation

For dynamic configuration, generate an SQL file at runtime:

```bash
# Generate SQL file at runtime
cat > /tmp/custom-init.sql << EOF
INSTALL delta;
LOAD delta;
ATTACH '${ENVIRONMENT}' AS main (TYPE UC_CATALOG);
EOF

export duckdb_init_sql_file=/tmp/custom-init.sql
```

## Migration from Hardcoded Configuration

If you were previously modifying the bundled `src/app/db/duckdb_init.sql` file:

1. Copy your modified SQL to a new file (e.g., `config/custom-init.sql`)
2. Set `duckdb_init_sql_file=config/custom-init.sql` in your environment
3. The bundled file will remain unchanged and serve as a fallback

## Troubleshooting

### Initialization Logs

The application logs detailed information about which initialization source is being used:

- `"Using initialization SQL from configured file: /path/to/file"` - Custom file config is active
- `"Using bundled initialization SQL file: /path/to/bundled"` - Fallback to bundled file
- `"No initialization SQL available from any source, using fallback"` - All sources failed

### Common Issues

1. **File not found**: Ensure the path in `duckdb_init_sql_file` exists and is readable
2. **Permission errors**: Ensure the application has read access to the custom SQL file
3. **SQL syntax errors**: Check logs for specific SQL statement failures
4. **Environment variable substitution**: Verify all referenced variables are set

### Validation

You can validate your custom SQL before deployment:

```bash
# Test environment variable substitution
envsubst < your-custom-init.sql

# Test SQL syntax (requires DuckDB CLI)
duckdb -c ".read your-custom-init.sql"
```
