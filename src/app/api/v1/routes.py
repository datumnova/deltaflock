from fastapi import APIRouter, HTTPException, Depends, Body, Query, Request, Header
import time
import logging
from typing import Optional, Dict, Any
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from ...services.query_service import QueryService
from ...services.query_cache_service import QueryCacheService
from ...telemetry.tracing import trace_async_function, TracingMixin
from ...config.settings import settings

router = APIRouter()
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class QueryHandler(TracingMixin):
    """Handler for query operations with detailed tracing"""

    def __init__(self):
        super().__init__()

    @trace_async_function("validate_query_input")
    async def validate_input(
        self, query: Optional[str], payload: Optional[Dict[str, Any]]
    ) -> str:
        """Validate and extract query from input parameters"""
        with self.tracer.start_as_current_span("input_validation") as span:
            self.log_and_trace("Starting query input validation", "debug")

            if query:
                span.set_attribute("input.source", "query_param")
                span.set_attribute("input.query_length", len(query))
                self.log_and_trace(
                    f"Query received via query parameter: {query[:100]}{'...' if len(query) > 100 else ''}"
                )
                return query

            if payload and payload.get("query"):
                extracted_query = payload.get("query")
                span.set_attribute("input.source", "json_body")
                span.set_attribute("input.query_length", len(extracted_query))
                span.set_attribute("input.payload_keys", ",".join(payload.keys()))
                self.log_and_trace(
                    f"Query received via JSON payload: {extracted_query[:100]}{'...' if len(extracted_query) > 100 else ''}"
                )
                return extracted_query

            span.set_status(Status(StatusCode.ERROR, "Missing query parameter"))
            self.log_and_trace("No query found in request", "warning")
            raise HTTPException(status_code=400, detail="Missing 'query' parameter")

    @trace_async_function("process_query_request")
    async def process_query(
        self, query: str, db_connection, request: Request
    ) -> Dict[str, Any]:
        """Process the query and return results with detailed tracing"""
        with self.tracer.start_as_current_span("query_processing") as span:
            # Add request metadata
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.url", str(request.url))
            span.set_attribute("http.user_agent", request.headers.get("user-agent", ""))
            span.set_attribute("query.text", query[:500])  # Truncate long queries
            span.set_attribute("query.length", len(query))
            span.set_attribute("query.hash", hash(query))

            self.log_and_trace(
                f"Processing query: {query[:200]}{'...' if len(query) > 200 else ''}"
            )

            start_time = time.time()

            try:
                # Database connection timing
                with self.tracer.start_as_current_span(
                    "database_connection"
                ) as db_span:
                    db_start = time.time()
                    with db_connection as connection:
                        db_connection_time = time.time() - db_start
                        db_span.set_attribute(
                            "db.connection_time_ms", db_connection_time * 1000
                        )
                        self.log_and_trace(
                            f"Database connection established in {db_connection_time:.3f}s"
                        )

                        # Query execution with cache configuration
                        query_service = QueryService(
                            connection,
                            enable_caching=settings.enable_query_cache,
                            cache_dir=settings.cache_directory,
                            cache_backend=settings.cache_backend,
                            redis_url=settings.redis_url,
                            cache_expiry_hours=settings.cache_expiry_hours,
                        )
                        query_start = time.time()
                        df = await query_service.execute_query(query)
                        query_execution_time = time.time() - query_start

                        # Add query results metadata
                        span.set_attribute(
                            "query.execution_time_ms", query_execution_time * 1000
                        )
                        span.set_attribute("result.rows", df.shape[0])
                        span.set_attribute("result.columns", df.shape[1])
                        span.set_attribute(
                            "result.column_names", ",".join(df.columns[:10])
                        )  # First 10 columns

                        self.log_and_trace(
                            f"Query executed successfully in {query_execution_time:.3f}s. "
                            f"Result: {df.shape[0]} rows, {df.shape[1]} columns"
                        )

                # Data conversion timing
                with self.tracer.start_as_current_span("data_conversion") as conv_span:
                    conversion_start = time.time()
                    data = df.to_dicts()
                    conversion_time = time.time() - conversion_start

                    conv_span.set_attribute(
                        "conversion.time_ms", conversion_time * 1000
                    )
                    conv_span.set_attribute(
                        "conversion.output_size_bytes", len(str(data))
                    )

                    self.log_and_trace(
                        f"Data conversion completed in {conversion_time:.3f}s"
                    )

                total_time = time.time() - start_time
                span.set_attribute("request.total_time_ms", total_time * 1000)
                span.set_status(Status(StatusCode.OK))

                self.log_and_trace(
                    f"Query request completed successfully in {total_time:.3f}s"
                )

                return {
                    "data": data,
                    "metadata": {
                        "query_execution_time_ms": query_execution_time * 1000,
                        "total_time_ms": total_time * 1000,
                        "rows_returned": df.shape[0],
                        "columns_returned": df.shape[1],
                        "cached": getattr(
                            df, "_from_cache", False
                        ),  # Will be set by QueryService if from cache
                    },
                }

            except Exception as e:
                error_time = time.time() - start_time
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                span.set_attribute("error.time_ms", error_time * 1000)

                self.log_and_trace(
                    f"Query processing failed after {error_time:.3f}s: {str(e)}",
                    "error",
                )
                raise


# Initialize the query handler
query_handler = QueryHandler()


def get_db_connection(request: Request):
    """Dependency to get the DuckDB connection from the session middleware."""
    session = request.state.duckdb_session
    return session.get_connection()


# Accept both GET and POST for convenience. GET uses query param; POST accepts JSON body {"query": "..."}
@router.post("/query", tags=["Query API"])
@trace_async_function("api_query_endpoint")
async def query_data(
    request: Request,
    query: str | None = Query(None),
    payload: dict | None = Body(None),
    db_connection=Depends(get_db_connection),
):
    """
    Execute a DuckDB query with comprehensive tracing and logging.

    Args:
        request: FastAPI request object
        query: Query string from URL parameter
        payload: JSON payload containing query
        db_connection: DuckDB connection dependency

    Returns:
        JSON response with query results and metadata
    """
    with tracer.start_as_current_span("query_endpoint") as span:
        # Add endpoint metadata
        span.set_attribute("endpoint.name", "query_data")
        span.set_attribute("endpoint.method", "POST")
        span.set_attribute("request.id", id(request))

        logger.info(
            f"Received query request from {request.client.host if request.client else 'unknown'}"
        )

        try:
            # Step 1: Validate input
            validated_query = await query_handler.validate_input(query, payload)

            # Step 2: Process query
            result = await query_handler.process_query(
                validated_query, db_connection, request
            )

            span.set_attribute("response.success", True)
            logger.info("Query request completed successfully")

            return result

        except HTTPException as e:
            span.set_attribute("response.success", False)
            span.set_attribute("response.error", str(e.detail))
            logger.warning(f"Query request failed with HTTP error: {e.detail}")
            raise
        except Exception as e:
            span.set_attribute("response.success", False)
            span.set_attribute("response.error", str(e))
            logger.error(f"Query request failed with unexpected error: {str(e)}")
            raise HTTPException(status_code=500, detail="Internal server error")


# Cache Management Endpoints
@router.get("/cache/stats", tags=["Cache Management"])
@trace_async_function("cache_stats_endpoint")
async def get_cache_statistics():
    """
    Get cache usage statistics including hit rate, total entries, and size.
    """
    with tracer.start_as_current_span("cache_stats") as span:
        try:
            if not settings.enable_query_cache:
                return {"cache_enabled": False, "message": "Query caching is disabled"}

            cache_service = QueryCacheService(
                cache_dir=settings.cache_directory,
                cache_expiry_hours=settings.cache_expiry_hours,
                backend=settings.cache_backend,
                redis_url=settings.redis_url,
                redis_ttl_seconds=settings.redis_cache_ttl_seconds,
            )

            stats = cache_service.get_cache_statistics()
            stats["cache_enabled"] = True
            stats["cache_directory"] = settings.cache_directory
            stats["cache_expiry_hours"] = settings.cache_expiry_hours
            stats["max_cache_size_mb"] = settings.max_cache_size_mb

            span.set_attribute("cache.enabled", True)
            span.set_attribute(
                "cache.total_entries", stats.get("total_cache_entries", 0)
            )
            span.set_attribute("cache.hit_rate", stats.get("hit_rate", 0))

            logger.info("Cache statistics retrieved successfully")
            return stats

        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(f"Error retrieving cache statistics: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to retrieve cache statistics"
            )


@router.post("/cache/clear", tags=["Cache Management"])
@trace_async_function("cache_clear_endpoint")
async def clear_cache(
    clear_type: str = Query(
        "expired", description="Type of clear operation: 'all' or 'expired'"
    ),
):
    """
    Clear cache entries. Options:
    - expired: Clear only expired entries
    - all: Clear all cache entries
    """
    with tracer.start_as_current_span("cache_clear") as span:
        try:
            if not settings.enable_query_cache:
                return {"cache_enabled": False, "message": "Query caching is disabled"}

            if clear_type not in ["expired", "all"]:
                raise HTTPException(
                    status_code=400,
                    detail="Invalid clear_type. Must be 'expired' or 'all'",
                )

            cache_service = QueryCacheService(
                cache_dir=settings.cache_directory,
                cache_expiry_hours=settings.cache_expiry_hours,
                backend=settings.cache_backend,
                redis_url=settings.redis_url,
                redis_ttl_seconds=settings.redis_cache_ttl_seconds,
            )

            if clear_type == "all":
                removed_count = cache_service.clear_all_cache()
                message = f"Cleared all cache entries ({removed_count} files removed)"
            else:
                removed_count = cache_service.clear_expired_cache()
                message = (
                    f"Cleared expired cache entries ({removed_count} files removed)"
                )

            span.set_attribute("cache.clear_type", clear_type)
            span.set_attribute("cache.removed_count", removed_count)

            logger.info(f"Cache clear operation completed: {message}")

            return {
                "success": True,
                "clear_type": clear_type,
                "removed_count": removed_count,
                "message": message,
            }

        except HTTPException:
            raise
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(f"Error clearing cache: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to clear cache")


@router.get("/cache/health", tags=["Cache Management"])
@trace_async_function("cache_health_endpoint")
async def get_cache_health():
    """
    Get cache health information including directory status and configuration.
    """
    with tracer.start_as_current_span("cache_health") as span:
        try:
            import os
            from pathlib import Path

            cache_backend = settings.cache_backend or "file"

            # Redis-backed cache health
            if cache_backend == "redis":
                try:
                    import redis as _redis

                    client = _redis.from_url(settings.redis_url)
                    ping = client.ping()

                    # Count keys matching our prefix using SCAN
                    key_pattern = "query:*"
                    key_count = 0
                    try:
                        cursor = 0
                        while True:
                            cursor, keys = client.scan(
                                cursor=cursor, match=key_pattern, count=1000
                            )
                            key_count += len(keys)
                            if cursor == 0:
                                break
                    except Exception:
                        key_count = None

                    health_info = {
                        "cache_enabled": settings.enable_query_cache,
                        "cache_backend": "redis",
                        "redis_url": settings.redis_url,
                        "redis_ping": bool(ping),
                        "cached_keys_count": key_count,
                        "cache_expiry_hours": settings.cache_expiry_hours,
                        "redis_cache_ttl_seconds": settings.redis_cache_ttl_seconds,
                    }

                    health_info["status"] = "healthy" if ping else "unhealthy"

                    span.set_attribute("cache.status", health_info["status"])
                    span.set_attribute("cache.key_count", key_count or 0)

                    logger.info(
                        f"Cache (redis) health check completed: {health_info['status']}"
                    )
                    return health_info

                except Exception as e:
                    span.set_attribute("cache.status", "unhealthy")
                    logger.error(f"Redis health check failed: {e}")
                    raise HTTPException(
                        status_code=500, detail=f"Redis health check failed: {e}"
                    )

            # File-backed cache health
            cache_dir = Path(settings.cache_directory)

            health_info = {
                "cache_enabled": settings.enable_query_cache,
                "cache_backend": "file",
                "cache_directory": str(cache_dir),
                "directory_exists": cache_dir.exists(),
                "directory_writable": cache_dir.exists()
                and os.access(cache_dir, os.W_OK),
                "cache_expiry_hours": settings.cache_expiry_hours,
                "max_cache_size_mb": settings.max_cache_size_mb,
                "cleanup_interval_hours": settings.cache_cleanup_interval_hours,
            }

            if cache_dir.exists():
                cache_files = list(cache_dir.glob("query_*.duckdb"))
                health_info["cache_file_count"] = len(cache_files)

                total_size = sum(f.stat().st_size for f in cache_files)
                health_info["total_cache_size_mb"] = total_size / (1024 * 1024)
                health_info["cache_directory_size_mb"] = sum(
                    f.stat().st_size for f in cache_dir.rglob("*") if f.is_file()
                ) / (1024 * 1024)
            else:
                health_info["cache_file_count"] = 0
                health_info["total_cache_size_mb"] = 0
                health_info["cache_directory_size_mb"] = 0

            # Determine overall health status
            if settings.enable_query_cache:
                if (
                    health_info["directory_exists"]
                    and health_info["directory_writable"]
                ):
                    health_info["status"] = "healthy"
                else:
                    health_info["status"] = "unhealthy"
                    health_info["issues"] = []
                    if not health_info["directory_exists"]:
                        health_info["issues"].append("Cache directory does not exist")
                    if not health_info["directory_writable"]:
                        health_info["issues"].append("Cache directory is not writable")
            else:
                health_info["status"] = "disabled"

            span.set_attribute("cache.status", health_info["status"])
            span.set_attribute("cache.file_count", health_info["cache_file_count"])
            span.set_attribute("cache.size_mb", health_info["total_cache_size_mb"])

            logger.info(f"Cache (file) health check completed: {health_info['status']}")
            return health_info

        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(f"Error checking cache health: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to check cache health")


@router.post("/cache/clear_by_table", tags=["Cache Management"])
@trace_async_function("cache_clear_by_table_endpoint")
async def clear_cache_by_table(
    table: str = Query(..., description="Table name to clear cache for"),
    admin_key: str | None = Header(
        None, description="Admin API key required for cache invalidation"
    ),
):
    """
    Clear cache entries that reference a specific table name.
    Uses `QueryCacheService.clear_cache_by_table` which inspects cache metadata.
    """
    with tracer.start_as_current_span("cache_clear_by_table") as span:
        try:
            if not settings.enable_query_cache:
                return {"cache_enabled": False, "message": "Query caching is disabled"}

            if not settings.admin_api_key:
                raise HTTPException(
                    status_code=403,
                    detail="Cache clear by table is disabled (admin key not configured)",
                )

            if not admin_key or admin_key != settings.admin_api_key:
                raise HTTPException(
                    status_code=401, detail="Invalid or missing admin key"
                )

            if not table or not table.strip():
                raise HTTPException(status_code=400, detail="Invalid 'table' parameter")
            cache_service = QueryCacheService(
                cache_dir=settings.cache_directory,
                cache_expiry_hours=settings.cache_expiry_hours,
                backend=settings.cache_backend,
                redis_url=settings.redis_url,
                redis_ttl_seconds=settings.redis_cache_ttl_seconds,
            )

            removed_count = cache_service.clear_cache_by_table(table.strip())

            span.set_attribute("cache.clear_by_table", table)
            span.set_attribute("cache.removed_count", removed_count)

            logger.info(
                f"Cleared {removed_count} cache entries referencing table: {table}"
            )

            return {
                "success": True,
                "table": table,
                "removed_count": removed_count,
                "message": f"Cleared {removed_count} cache entries for table '{table}'",
            }

        except HTTPException:
            raise
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            logger.error(f"Error clearing cache by table: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to clear cache by table"
            )
