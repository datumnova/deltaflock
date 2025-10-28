from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Database configuration
    database_url: str = "duckdb:///:memory:"
    duckdb_path: str = "path/to/your/duckdb/file"

    # Unity Catalog configuration
    uc_catalog_endpoint: str = "https://localhost:8080"  # nosec - default local endpoint
    uc_catalog_token: str = ""  # Set via UC_CATALOG_TOKEN environment variable

    # Azure configuration
    azure_storage_account_name: str = ""  # Set via AZURE_STORAGE_ACCOUNT_NAME
    azure_storage_account_key: str = ""  # Set via AZURE_STORAGE_ACCOUNT_KEY
    azure_account_name: str = ""  # Set via AZURE_ACCOUNT_NAME
    azure_client_id: str = ""  # Set via AZURE_CLIENT_ID
    azure_client_secret: str = ""  # Set via AZURE_CLIENT_SECRET
    azure_tenant_id: str = ""  # Set via AZURE_TENANT_ID

    # Telemetry configuration
    telemetry_enabled: bool = True
    telemetry_endpoint: str = "https://your_telemetry_endpoint"
    telemetry_service_name: str = "duckdb-delta-api"
    telemetry_service_version: str = "1.0.0"
    opentelemetry_service_name: str = "duckdb-delta-api"
    opentelemetry_endpoint: str = "http://localhost:4317"

    # Cache configuration
    enable_query_cache: bool = True
    cache_directory: str = "./query_cache"
    cache_expiry_hours: int = 24
    max_cache_size_mb: int = 1000  # Maximum cache size in MB
    cache_cleanup_interval_hours: int = 6  # How often to clean up expired cache entries
    # Cache backend: 'file' (default) uses per-query duckdb files, 'redis' stores JSON results in Redis
    cache_backend: str = "file"
    # Redis configuration (used when cache_backend == 'redis')
    redis_url: str = "redis://localhost:6379/0"
    # Redis TTL (seconds) - defaults to cache_expiry_hours * 3600 if not provided
    redis_cache_ttl_seconds: int | None = None
    # Database initialization configuration
    duckdb_init_sql_file: str | None = None  # Path to custom initialization SQL file

    # Admin
    admin_api_key: str | None = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        # Allow extra fields to avoid validation errors
        extra = "ignore"


settings = Settings()
