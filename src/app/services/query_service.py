import asyncio
import time
from fastapi import HTTPException
from duckdb import DuckDBPyConnection
from opentelemetry.trace import Status, StatusCode
from ..telemetry.tracing import trace_async_function, TracingMixin
from .query_cache_service import QueryCacheService


class QueryService(TracingMixin):
    """Service that executes DuckDB queries with comprehensive tracing.

    Accepts a DuckDBPyConnection (the connection yielded by the session dependency)
    and runs blocking DB calls in a thread to avoid blocking the event loop.
    """

    def __init__(
        self,
        session: DuckDBPyConnection,
        enable_caching: bool = True,
        cache_dir: str = "./query_cache",
        cache_backend: str = "file",
        redis_url: str | None = None,
        cache_expiry_hours: int = 24,
    ):
        super().__init__()
        self.session = session
        self.enable_caching = enable_caching

        # Initialize cache service if caching is enabled
        if self.enable_caching:
            self.cache_service = QueryCacheService(
                cache_dir=cache_dir,
                cache_expiry_hours=cache_expiry_hours,
                backend=cache_backend,
                redis_url=redis_url,
            )
            self.log_and_trace(
                f"QueryService initialized with caching enabled (cache_dir: {cache_dir})"
            )
        else:
            self.cache_service = None
            self.log_and_trace("QueryService initialized with caching disabled")

    @trace_async_function("execute_duckdb_query")
    async def execute_query(self, query: str):
        """Execute a DuckDB query with caching support and detailed timing and tracing"""
        with self.tracer.start_as_current_span("query_execution") as span:
            # Add query metadata
            span.set_attribute(
                "db.statement", query[:1000]
            )  # Truncate very long queries
            span.set_attribute("db.operation", self._extract_operation_type(query))
            span.set_attribute("db.system", "duckdb")
            span.set_attribute("query.character_count", len(query))
            span.set_attribute("query.word_count", len(query.split()))
            span.set_attribute("query.caching_enabled", self.enable_caching)

            self.log_and_trace(
                f"Starting query execution: {query[:100]}{'...' if len(query) > 100 else ''}"
            )

            # Enforce read-only policy: block queries that manipulate data or schema
            operation_type = self._extract_operation_type(query)
            if operation_type not in ("SELECT", "CTE/WITH"):
                self.log_and_trace(
                    f"Blocked non-read-only query of type: {operation_type}", "warning"
                )
                raise HTTPException(
                    status_code=403,
                    detail="Only read-only SELECT queries are allowed through this API",
                )

            start_time = time.time()

            try:
                # Step 1: Check cache if enabled
                cached_result = None
                if self.enable_caching and self.cache_service:
                    with self.tracer.start_as_current_span("cache_check") as cache_span:
                        cache_check_start = time.time()

                        cached_result = await self.cache_service.get_cached_result(
                            query
                        )

                        cache_check_time = time.time() - cache_check_start
                        cache_span.set_attribute(
                            "cache.check_time_ms", cache_check_time * 1000
                        )
                        cache_span.set_attribute("cache.hit", cached_result is not None)

                        if cached_result is not None:
                            span.set_attribute("query.source", "cache")
                            span.set_attribute("query.cache_hit", True)
                            span.set_attribute(
                                "result.row_count", cached_result.shape[0]
                            )
                            span.set_attribute(
                                "result.column_count", cached_result.shape[1]
                            )

                            total_time = time.time() - start_time
                            span.set_attribute("query.total_time_ms", total_time * 1000)
                            span.set_status(Status(StatusCode.OK))

                            self.log_and_trace(
                                f"Cache hit! Returning cached result with {cached_result.shape[0]} rows, "
                                f"{cached_result.shape[1]} columns in {total_time:.3f}s"
                            )

                            return cached_result
                        else:
                            span.set_attribute("query.cache_hit", False)
                            self.log_and_trace("Cache miss, executing query against UC")

                # Step 2: Execute query if no cache hit
                span.set_attribute("query.source", "database")

                # Pre-execution analysis
                with self.tracer.start_as_current_span(
                    "query_analysis"
                ) as analysis_span:
                    operation_type = self._extract_operation_type(query)
                    estimated_complexity = self._estimate_query_complexity(query)

                    analysis_span.set_attribute("query.operation_type", operation_type)
                    analysis_span.set_attribute(
                        "query.estimated_complexity", estimated_complexity
                    )

                    self.log_and_trace(
                        f"Query analysis: type={operation_type}, complexity={estimated_complexity}"
                    )

                # Execute query in thread pool to avoid blocking
                def _run_query():
                    """Internal function to run the blocking query"""
                    thread_start = time.time()

                    try:
                        # Execute the query
                        result = self.session.execute(query)

                        # Convert to Polars DataFrame
                        df = result.pl()

                        thread_time = time.time() - thread_start
                        return df, thread_time

                    except Exception as e:
                        thread_time = time.time() - thread_start
                        raise Exception(
                            f"Query execution failed after {thread_time:.3f}s: {str(e)}"
                        )

                with self.tracer.start_as_current_span(
                    "thread_execution"
                ) as thread_span:
                    self.log_and_trace("Executing query in thread pool")
                    thread_start = time.time()

                    df, query_thread_time = await asyncio.to_thread(_run_query)

                    thread_total_time = time.time() - thread_start

                    # Add thread execution metrics
                    thread_span.set_attribute(
                        "thread.execution_time_ms", query_thread_time * 1000
                    )
                    thread_span.set_attribute(
                        "thread.total_time_ms", thread_total_time * 1000
                    )
                    thread_span.set_attribute(
                        "thread.overhead_ms",
                        (thread_total_time - query_thread_time) * 1000,
                    )

                # Step 3: Store result in cache if enabled and query was successful
                if self.enable_caching and self.cache_service and df is not None:
                    with self.tracer.start_as_current_span("cache_store") as store_span:
                        store_start = time.time()

                        # Only cache SELECT queries (not DDL/DML operations)
                        if self._should_cache_query(query):
                            store_success = await self.cache_service.store_result(
                                query, df
                            )

                            store_time = time.time() - store_start
                            store_span.set_attribute(
                                "cache.store_time_ms", store_time * 1000
                            )
                            store_span.set_attribute(
                                "cache.store_success", store_success
                            )

                            if store_success:
                                self.log_and_trace(
                                    f"Successfully cached query result in {store_time:.3f}s"
                                )
                            else:
                                self.log_and_trace(
                                    f"Failed to cache query result after {store_time:.3f}s",
                                    "warning",
                                )
                        else:
                            store_span.set_attribute("cache.store_skipped", True)
                            store_span.set_attribute(
                                "cache.skip_reason", "query_type_not_cacheable"
                            )
                            self.log_and_trace(
                                "Skipping cache storage for non-SELECT query"
                            )

                # Analyze results
                with self.tracer.start_as_current_span(
                    "result_analysis"
                ) as result_span:
                    result_analysis_start = time.time()

                    rows, cols = df.shape
                    column_types = [str(dtype) for dtype in df.dtypes]
                    memory_usage = (
                        df.estimated_size("mb") if hasattr(df, "estimated_size") else 0
                    )

                    result_span.set_attribute("result.row_count", rows)
                    result_span.set_attribute("result.column_count", cols)
                    result_span.set_attribute(
                        "result.column_types", ",".join(column_types[:10])
                    )  # First 10 types
                    result_span.set_attribute(
                        "result.estimated_memory_mb", memory_usage
                    )

                    result_analysis_time = time.time() - result_analysis_start
                    result_span.set_attribute(
                        "analysis.time_ms", result_analysis_time * 1000
                    )

                    self.log_and_trace(
                        f"Query results analyzed: {rows} rows, {cols} columns, "
                        f"~{memory_usage:.2f}MB, analysis took {result_analysis_time:.3f}s"
                    )

                total_time = time.time() - start_time

                # Final span attributes
                span.set_attribute("query.total_time_ms", total_time * 1000)
                span.set_attribute("query.rows_returned", rows)
                span.set_attribute("query.success", True)
                span.set_status(Status(StatusCode.OK))

                self.log_and_trace(
                    f"Query executed successfully in {total_time:.3f}s total "
                    f"({query_thread_time:.3f}s execution, {rows} rows returned)"
                )

                return df

            except Exception as e:
                error_time = time.time() - start_time

                # Error handling with detailed tracing
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                span.set_attribute("error.time_ms", error_time * 1000)
                span.set_attribute("query.success", False)

                self.log_and_trace(
                    f"Query execution failed after {error_time:.3f}s: {str(e)}",
                    "error",
                    error_type=type(e).__name__,
                )

                # Bubble up as HTTPException so FastAPI returns a proper response
                raise HTTPException(status_code=400, detail=str(e))

    def _should_cache_query(self, query: str) -> bool:
        """Determine if a query should be cached based on its type and characteristics"""
        query_upper = query.strip().upper()

        # Only cache SELECT queries
        if not query_upper.startswith("SELECT") and "SELECT" not in query_upper:
            return False

        # Don't cache queries with non-deterministic functions
        non_deterministic_functions = [
            "NOW()",
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
            "CURRENT_TIME",
            "RANDOM()",
            "RAND()",
            "UUID()",
            "NEWID()",
        ]

        for func in non_deterministic_functions:
            if func in query_upper:
                return False

        # Don't cache very simple queries (less than 20 characters)
        if len(query.strip()) < 20:
            return False

        return True

    def _extract_operation_type(self, query: str) -> str:
        """Extract the primary operation type from a SQL query"""
        query_upper = query.strip().upper()

        if query_upper.startswith("SELECT"):
            return "SELECT"
        elif query_upper.startswith("INSERT"):
            return "INSERT"
        elif query_upper.startswith("UPDATE"):
            return "UPDATE"
        elif query_upper.startswith("DELETE"):
            return "DELETE"
        elif query_upper.startswith("CREATE"):
            return "CREATE"
        elif query_upper.startswith("DROP"):
            return "DROP"
        elif query_upper.startswith("ALTER"):
            return "ALTER"
        elif query_upper.startswith("WITH"):
            return "CTE/WITH"
        else:
            return "OTHER"

    def _estimate_query_complexity(self, query: str) -> str:
        """Estimate query complexity based on keywords and structure"""
        query_upper = query.upper()
        complexity_score = 0

        # Basic operations
        if "JOIN" in query_upper:
            complexity_score += query_upper.count("JOIN") * 2
        if "UNION" in query_upper:
            complexity_score += query_upper.count("UNION") * 2
        if "SUBQUERY" in query_upper or "(" in query:
            complexity_score += query.count("(")
        if "GROUP BY" in query_upper:
            complexity_score += 2
        if "ORDER BY" in query_upper:
            complexity_score += 1
        if "WINDOW" in query_upper or "OVER" in query_upper:
            complexity_score += 3

        # Length-based complexity
        if len(query) > 1000:
            complexity_score += 2
        elif len(query) > 500:
            complexity_score += 1

        if complexity_score >= 10:
            return "HIGH"
        elif complexity_score >= 5:
            return "MEDIUM"
        else:
            return "LOW"
