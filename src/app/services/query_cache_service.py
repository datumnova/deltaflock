import hashlib
import time
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import json
import polars as pl
from duckdb import connect
from opentelemetry.trace import Status, StatusCode
from ..telemetry.tracing import trace_async_function, TracingMixin, trace_function

try:
    import redis
except Exception:
    redis = None


class QueryCacheService(TracingMixin):
    """
    Service to handle query result caching using DuckDB persistent files.

    Each unique query gets its own DuckDB file for persistence.
    Cache keys are generated using SHA-256 hash of the normalized query.
    """

    def __init__(
        self,
        cache_dir: str = "./query_cache",
        cache_expiry_hours: int = 24,
        backend: str = "file",
        redis_url: str | None = None,
        redis_ttl_seconds: int | None = None,
    ):
        super().__init__()
        self.cache_dir = Path(cache_dir)
        self.cache_expiry_hours = cache_expiry_hours
        self.backend = backend or "file"
        self.redis_url = redis_url
        # TTL in seconds for redis cache entries
        self.redis_ttl_seconds = (
            redis_ttl_seconds
            if redis_ttl_seconds is not None
            else int(cache_expiry_hours * 3600)
        )
        self.redis_client = None

        if self.backend == "redis":
            if redis is None:
                raise RuntimeError(
                    "redis package not installed; add 'redis' to your dependencies to use redis cache backend"
                )
            self.redis_client = redis.from_url(
                self.redis_url or "redis://localhost:6379/0"
            )
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Initialize cache metadata file
        self.metadata_file = self.cache_dir / "cache_metadata.json"
        self._ensure_metadata_file()

        self.log_and_trace(
            f"QueryCacheService initialized with cache_dir: {self.cache_dir}"
        )
        if self.backend == "redis":
            self.log_and_trace(
                f"QueryCacheService running in REDIS mode against {self.redis_url}"
            )

    def _ensure_metadata_file(self):
        """Ensure cache metadata file exists"""
        if not self.metadata_file.exists():
            initial_metadata = {
                "created_at": datetime.now().isoformat(),
                "cache_entries": {},
                "statistics": {
                    "total_queries_cached": 0,
                    "cache_hits": 0,
                    "cache_misses": 0,
                    "total_size_bytes": 0,
                },
            }
            with open(self.metadata_file, "w") as f:
                json.dump(initial_metadata, f, indent=2)

    @trace_function("generate_query_hash")
    def _generate_query_hash(self, query: str) -> str:
        """Generate a consistent hash for a query string"""
        with self.tracer.start_as_current_span("normalize_and_hash_query") as span:
            # Normalize the query to ensure consistent hashing
            normalized_query = self._normalize_query(query)

            # Generate SHA-256 hash
            query_hash = hashlib.sha256(normalized_query.encode("utf-8")).hexdigest()

            span.set_attribute("query.original_length", len(query))
            span.set_attribute("query.normalized_length", len(normalized_query))
            span.set_attribute("query.hash", query_hash)

            self.log_and_trace(
                f"Generated query hash: {query_hash} for normalized query length: {len(normalized_query)}"
            )

            return query_hash

    def _extract_table_names(self, query: str) -> list[str]:
        """Extract referenced table names from a SQL query using simple heuristics.

        This is intentionally conservative and heuristic-based: it looks for FROM and JOIN
        clauses and extracts the first identifier that follows. It does not attempt to
        fully parse SQL (use a SQL parser for production needs).
        """
        import re

        normalized = query.lower()
        # Find FROM <identifier> and JOIN <identifier>
        patterns = [
            r"\bfrom\s+([`\"]?\w[\w\.]*[`\"]?)",
            r"\bjoin\s+([`\"]?\w[\w\.]*[`\"]?)",
        ]
        names = []
        for pat in patterns:
            for m in re.finditer(pat, normalized, flags=re.IGNORECASE):
                name = m.group(1)
                # Strip quotes/backticks
                name = name.strip('"`')
                # Remove alias if present (e.g., table t)
                name = name.split()[0]
                if name and name not in names:
                    names.append(name.replace(".", "__"))

        return names

    def _normalize_query(self, query: str) -> str:
        """Normalize query string for consistent hashing"""
        # Remove extra whitespace, convert to lowercase, remove comments
        import re

        # Remove SQL comments
        query = re.sub(r"--.*?\n", "", query)
        query = re.sub(r"/\*.*?\*/", "", query, flags=re.DOTALL)

        # Normalize whitespace
        query = " ".join(query.split())

        # Convert to lowercase for case-insensitive caching
        query = query.lower().strip()

        return query

    @trace_function("get_cache_file_path")
    def _get_cache_file_path(
        self, query_hash: str, table_names: list[str] | None = None
    ) -> Path:
        """Get the file path for a cached query.

        If `table_names` are provided, include a sanitized, short representation of them
        in the filename to enable table-scoped invalidation (e.g. `query_table1_table2_{hash}.duckdb`).
        """
        if table_names:
            # Join table names; guard against overly long filenames by truncating the tables part
            tables_part = "_".join(table_names)
            if len(tables_part) > 80:
                # Shorten by hashing the joined table names
                tables_part = hashlib.sha256(tables_part.encode("utf-8")).hexdigest()[
                    :16
                ]
            filename = f"query_{tables_part}_{query_hash}.duckdb"
        else:
            filename = f"query_{query_hash}.duckdb"

        return self.cache_dir / filename

    @trace_function("is_cache_valid")
    def _is_cache_valid(self, cache_file: Path) -> bool:
        """Check if cache file is valid and not expired"""
        if not cache_file.exists():
            return False

        # Check file age
        file_stat = cache_file.stat()
        file_age = datetime.now() - datetime.fromtimestamp(file_stat.st_mtime)

        is_valid = file_age < timedelta(hours=self.cache_expiry_hours)

        self.log_and_trace(
            f"Cache file {cache_file.name} age: {file_age}, valid: {is_valid} "
            f"(expiry: {self.cache_expiry_hours}h)"
        )

        return is_valid

    @trace_async_function("check_cache")
    async def get_cached_result(self, query: str) -> Optional[pl.DataFrame]:
        """
        Check if query result exists in cache and return it if valid

        Args:
            query: SQL query string

        Returns:
            Polars DataFrame if cache hit, None if cache miss
        """
        with self.tracer.start_as_current_span("cache_lookup") as span:
            query_hash = self._generate_query_hash(query)
            table_names = self._extract_table_names(query)

            span.set_attribute("cache.query_hash", query_hash)
            span.set_attribute(
                "cache.tables", ",".join(table_names) if table_names else ""
            )

            # Redis backend: store JSON payloads under key "query:{hash}"
            if self.backend == "redis":
                try:
                    key = f"query:{query_hash}"
                    raw = self.redis_client.get(key)
                    if not raw:
                        span.set_attribute("cache.result", "miss")
                        self._update_cache_statistics("cache_miss")
                        self.log_and_trace(
                            f"Redis cache miss for query hash: {query_hash}"
                        )
                        return None

                    with self.tracer.start_as_current_span(
                        "load_from_redis"
                    ) as load_span:
                        load_start = time.time()
                        try:
                            payload = json.loads(raw)
                            # payload expected to be list-of-dicts (rows)
                            df = pl.from_dicts(payload)
                            # mark origin
                            setattr(df, "_from_cache", True)

                            load_time = time.time() - load_start
                            load_span.set_attribute(
                                "cache.load_time_ms", load_time * 1000
                            )
                            load_span.set_attribute("cache.rows_loaded", df.shape[0])
                            load_span.set_attribute("cache.columns_loaded", df.shape[1])

                        except Exception as e:
                            load_span.set_status(Status(StatusCode.ERROR, str(e)))
                            self.log_and_trace(
                                f"Error decoding redis payload: {e}", "error"
                            )
                            # Remove corrupted key
                            try:
                                self.redis_client.delete(key)
                            except Exception:
                                pass
                            return None

                    span.set_attribute("cache.result", "hit")
                    self._update_cache_statistics("cache_hit")
                    self.log_and_trace(
                        f"Redis cache hit for query hash: {query_hash}. Loaded {df.shape[0]} rows, {df.shape[1]} columns"
                    )

                    return df

                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.set_attribute("cache.error", str(e))
                    self.log_and_trace(
                        f"Error loading from redis cache: {str(e)}", "error"
                    )
                    return None

            # FILE backend (original behavior)
            cache_file = self._get_cache_file_path(query_hash, table_names)

            span.set_attribute("cache.file_path", str(cache_file))
            span.set_attribute("cache.file_exists", cache_file.exists())

            if not self._is_cache_valid(cache_file):
                span.set_attribute("cache.result", "miss")
                self._update_cache_statistics("cache_miss")
                self.log_and_trace(f"Cache miss for query hash: {query_hash}")
                return None

            try:
                # Load data from cache file
                with self.tracer.start_as_current_span("load_from_cache") as load_span:
                    load_start = time.time()

                    conn = connect(str(cache_file))
                    try:
                        # Get the cached table (assuming single table per cache file)
                        tables = conn.execute(
                            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
                        ).fetchall()

                        if not tables:
                            load_span.set_status(
                                Status(
                                    StatusCode.ERROR, "No tables found in cache file"
                                )
                            )
                            self.log_and_trace(
                                f"No tables found in cache file: {cache_file}",
                                "warning",
                            )
                            return None

                        table_name = tables[0][0]
                        df = conn.execute(f"SELECT * FROM {table_name}").pl()
                        setattr(df, "_from_cache", True)

                        load_time = time.time() - load_start
                        load_span.set_attribute("cache.load_time_ms", load_time * 1000)
                        load_span.set_attribute("cache.table_name", table_name)
                        load_span.set_attribute("cache.rows_loaded", df.shape[0])
                        load_span.set_attribute("cache.columns_loaded", df.shape[1])

                    finally:
                        conn.close()

                span.set_attribute("cache.result", "hit")
                span.set_attribute("cache.rows_returned", df.shape[0])
                span.set_attribute("cache.columns_returned", df.shape[1])

                self._update_cache_statistics("cache_hit")
                self.log_and_trace(
                    f"Cache hit for query hash: {query_hash}. "
                    f"Loaded {df.shape[0]} rows, {df.shape[1]} columns in {load_time:.3f}s"
                )

                return df

            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.set_attribute("cache.error", str(e))

                self.log_and_trace(f"Error loading from cache: {str(e)}", "error")

                # If cache file is corrupted, remove it
                try:
                    cache_file.unlink()
                    self.log_and_trace(f"Removed corrupted cache file: {cache_file}")
                except Exception:
                    pass

                return None

    @trace_async_function("store_cache")
    async def store_result(self, query: str, result_df: pl.DataFrame) -> bool:
        """
        Store query result in cache

        Args:
            query: SQL query string
            result_df: Polars DataFrame with query results

        Returns:
            True if successfully stored, False otherwise
        """
        with self.tracer.start_as_current_span("cache_store") as span:
            query_hash = self._generate_query_hash(query)
            table_names = self._extract_table_names(query)

            span.set_attribute("cache.query_hash", query_hash)
            span.set_attribute("cache.rows_to_store", result_df.shape[0])
            span.set_attribute("cache.columns_to_store", result_df.shape[1])

            # Redis backend: store JSON array of row dicts
            if self.backend == "redis":
                try:
                    with self.tracer.start_as_current_span(
                        "write_to_redis"
                    ) as write_span:
                        store_start = time.time()
                        key = f"query:{query_hash}"
                        payload = result_df.to_dicts()
                        raw = json.dumps(payload, default=str)
                        # Set with TTL
                        self.redis_client.set(key, raw, ex=self.redis_ttl_seconds)
                        store_time = time.time() - store_start
                        write_span.set_attribute(
                            "cache.store_time_ms", store_time * 1000
                        )

                    # Store metadata for table-scoped invalidation and stats
                    try:
                        meta_key = f"query_meta:{query_hash}"
                        meta = {
                            "query_hash": query_hash,
                            "created_at": datetime.now().isoformat(),
                            "row_count": result_df.shape[0],
                            "column_count": result_df.shape[1],
                            "query_preview": query[:500],
                            "tables": table_names or [],
                        }
                        self.redis_client.set(
                            meta_key,
                            json.dumps(meta, default=str),
                            ex=self.redis_ttl_seconds,
                        )

                        # For each table referenced, maintain a Redis set of query_hashes
                        for t in table_names or []:
                            table_key = f"table:{t}"
                            self.redis_client.sadd(table_key, query_hash)
                            # ensure the table set has a TTL at least as long as entries (best-effort)
                            try:
                                self.redis_client.expire(
                                    table_key, self.redis_ttl_seconds
                                )
                            except Exception:
                                pass

                    except Exception as _meta_e:
                        # Metadata errors shouldn't prevent primary cache store
                        self.log_and_trace(
                            f"Failed to persist redis cache metadata for {query_hash}: {_meta_e}",
                            "warning",
                        )

                    # Update basic metadata file/stat counters for visibility
                    try:
                        self._update_cache_statistics(
                            "cache_store", len(raw.encode("utf-8"))
                        )
                    except Exception:
                        pass

                    self.log_and_trace(
                        f"Successfully stored query {query_hash} in redis ({len(payload)} rows)"
                    )

                    return True

                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.set_attribute("cache.store_success", False)
                    span.set_attribute("cache.error", str(e))

                    self.log_and_trace(
                        f"Error storing result in redis cache: {str(e)}", "error"
                    )
                    return False

            # FILE backend (original behavior)
            cache_file = self._get_cache_file_path(query_hash, table_names)

            span.set_attribute("cache.file_path", str(cache_file))

            try:
                with self.tracer.start_as_current_span("write_to_cache") as write_span:
                    store_start = time.time()

                    # Create connection to cache file
                    conn = connect(str(cache_file))
                    try:
                        # Create a table with the results
                        table_name = "cached_result"

                        # Write DataFrame to DuckDB - we need to register the DataFrame first
                        conn.register("result_df", result_df)
                        conn.execute(
                            f"CREATE TABLE {table_name} AS SELECT * FROM result_df"
                        )

                        # Add metadata about the cache entry
                        metadata_table = "cache_metadata"
                        conn.execute(f"""
                            CREATE TABLE {metadata_table} AS SELECT
                                '{query_hash}' as query_hash,
                                '{datetime.now().isoformat()}' as created_at,
                                {result_df.shape[0]} as row_count,
                                {result_df.shape[1]} as column_count,
                                '{query[:500]}' as query_preview
                        """)

                        store_time = time.time() - store_start

                        write_span.set_attribute(
                            "cache.store_time_ms", store_time * 1000
                        )
                        write_span.set_attribute("cache.table_name", table_name)

                    finally:
                        conn.close()

                # Update global cache metadata (include table names for invalidation)
                self._update_cache_metadata(
                    query_hash, query, result_df, cache_file, table_names
                )

                file_size = cache_file.stat().st_size
                span.set_attribute("cache.file_size_bytes", file_size)
                span.set_attribute("cache.store_success", True)

                self._update_cache_statistics("cache_store", file_size)

                self.log_and_trace(
                    f"Successfully cached query result: {query_hash}. "
                    f"Stored {result_df.shape[0]} rows, {result_df.shape[1]} columns "
                    f"in {store_time:.3f}s, file size: {file_size} bytes"
                )

                return True

            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.set_attribute("cache.store_success", False)
                span.set_attribute("cache.error", str(e))

                self.log_and_trace(f"Error storing result in cache: {str(e)}", "error")

                # Clean up partial cache file
                try:
                    if cache_file.exists():
                        cache_file.unlink()
                except Exception:
                    pass

                return False

    def _update_cache_metadata(
        self,
        query_hash: str,
        query: str,
        result_df: pl.DataFrame,
        cache_file: Path,
        table_names: list[str] | None = None,
    ):
        """Update global cache metadata"""
        try:
            with open(self.metadata_file, "r") as f:
                metadata = json.load(f)

            metadata["cache_entries"][query_hash] = {
                "created_at": datetime.now().isoformat(),
                "query_preview": query[:200],
                "row_count": result_df.shape[0],
                "column_count": result_df.shape[1],
                "file_size_bytes": cache_file.stat().st_size,
                "file_path": str(cache_file),
                "tables": table_names or [],
            }

            with open(self.metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            self.log_and_trace(f"Error updating cache metadata: {str(e)}", "warning")

    def _update_cache_statistics(self, operation: str, size_bytes: int = 0):
        """Update cache statistics"""
        try:
            with open(self.metadata_file, "r") as f:
                metadata = json.load(f)

            stats = metadata["statistics"]

            if operation == "cache_hit":
                stats["cache_hits"] += 1
            elif operation == "cache_miss":
                stats["cache_misses"] += 1
            elif operation == "cache_store":
                stats["total_queries_cached"] += 1
                stats["total_size_bytes"] += size_bytes

            with open(self.metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            self.log_and_trace(f"Error updating cache statistics: {str(e)}", "warning")

    @trace_function("get_cache_statistics")
    def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache usage statistics"""
        try:
            # Redis backend: derive statistics from redis and optional metadata file
            if self.backend == "redis":
                stats = {
                    "cache_hits": 0,
                    "cache_misses": 0,
                    "total_queries_cached": 0,
                    "total_size_bytes": 0,
                }

                # Try to read persisted metadata if present
                try:
                    if self.metadata_file.exists():
                        with open(self.metadata_file, "r") as f:
                            metadata = json.load(f)
                        stats.update(metadata.get("statistics", {}))
                except Exception:
                    pass

                # Count redis keys matching our prefix
                try:
                    cursor = 0
                    key_count = 0
                    while True:
                        cursor, keys = self.redis_client.scan(
                            cursor=cursor, match="query:*", count=1000
                        )
                        key_count += len(keys)
                        if cursor == 0:
                            break
                    stats["total_cache_entries"] = key_count
                except Exception:
                    stats["total_cache_entries"] = stats.get("total_queries_cached", 0)

                total_requests = stats.get("cache_hits", 0) + stats.get(
                    "cache_misses", 0
                )
                stats["hit_rate"] = (
                    (stats.get("cache_hits", 0) / total_requests)
                    if total_requests > 0
                    else 0
                )
                stats["cache_dir_size_mb"] = 0
                return stats

            # Default: file-backed cache statistics
            with open(self.metadata_file, "r") as f:
                metadata = json.load(f)

            stats = metadata["statistics"].copy()

            # Add derived statistics
            total_requests = stats["cache_hits"] + stats["cache_misses"]
            stats["hit_rate"] = (
                stats["cache_hits"] / total_requests if total_requests > 0 else 0
            )
            stats["total_cache_entries"] = len(metadata["cache_entries"])
            stats["cache_dir_size_mb"] = sum(
                f.stat().st_size for f in self.cache_dir.rglob("*.duckdb")
            ) / (1024 * 1024)

            return stats

        except Exception as e:
            self.log_and_trace(f"Error getting cache statistics: {str(e)}", "warning")
            return {}

    @trace_function("clear_expired_cache")
    def clear_expired_cache(self) -> int:
        """Clear expired cache entries and return count of removed files"""
        removed_count = 0
        try:
            if self.backend == "redis":
                # Redis uses TTL to expire keys; expired entries should be automatically removed.
                self.log_and_trace(
                    "Redis backend: expired entries handled by TTL; nothing to clear"
                )
                return 0

            for cache_file in self.cache_dir.glob("query_*.duckdb"):
                if not self._is_cache_valid(cache_file):
                    cache_file.unlink()
                    removed_count += 1
                    self.log_and_trace(f"Removed expired cache file: {cache_file.name}")

            # Update metadata file to remove expired entries
            with open(self.metadata_file, "r") as f:
                metadata = json.load(f)

            valid_entries = {}
            for query_hash, entry in metadata["cache_entries"].items():
                cache_file = self._get_cache_file_path(query_hash)
                if cache_file.exists():
                    valid_entries[query_hash] = entry

            metadata["cache_entries"] = valid_entries

            with open(self.metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            self.log_and_trace(f"Error clearing expired cache: {str(e)}", "warning")

        self.log_and_trace(f"Cleared {removed_count} expired cache entries")
        return removed_count

    @trace_function("clear_all_cache")
    def clear_all_cache(self) -> int:
        """Clear all cache entries and return count of removed files"""
        removed_count = 0

        try:
            if self.backend == "redis":
                # Delete keys matching our prefix
                try:
                    cursor = 0
                    # Remove query keys and metadata keys
                    for pattern in ("query:*", "query_meta:*"):
                        cursor = 0
                        while True:
                            cursor, keys = self.redis_client.scan(
                                cursor=cursor, match=pattern, count=1000
                            )
                            if keys:
                                # Redis delete can accept many keys; guard by chunking
                                try:
                                    self.redis_client.delete(*keys)
                                except Exception:
                                    for k in keys:
                                        try:
                                            self.redis_client.delete(k)
                                        except Exception:
                                            pass
                                removed_count += len(keys)
                            if cursor == 0:
                                break

                    # Also remove table sets
                    cursor = 0
                    while True:
                        cursor, keys = self.redis_client.scan(
                            cursor=cursor, match="table:*", count=1000
                        )
                        if keys:
                            try:
                                self.redis_client.delete(*keys)
                            except Exception:
                                for k in keys:
                                    try:
                                        self.redis_client.delete(k)
                                    except Exception:
                                        pass
                            removed_count += len(keys)
                        if cursor == 0:
                            break
                except Exception as e:
                    self.log_and_trace(f"Error clearing redis cache: {e}", "warning")
                    raise

                # Note: metadata file left untouched in file path
            else:
                for cache_file in self.cache_dir.glob("query_*.duckdb"):
                    cache_file.unlink()
                    removed_count += 1
                    self.log_and_trace(f"Removed cache file: {cache_file.name}")

                # Reset metadata
                self._ensure_metadata_file()

        except Exception as e:
            self.log_and_trace(f"Error clearing all cache: {str(e)}", "warning")

        self.log_and_trace(f"Cleared all cache entries ({removed_count} files)")
        return removed_count

    @trace_function("clear_cache_by_table")
    def clear_cache_by_table(self, table_name: str) -> int:
        """Clear cache entries that reference a given table name.

        This uses the metadata file to find cache entries that list `table_name` in their
        `tables` attribute. Returns the number of removed cache files.
        """
        removed_count = 0

        try:
            if self.backend == "redis":
                # Use the per-table set to find query hashes and delete them
                try:
                    table_key = f"table:{table_name}"
                    members = self.redis_client.smembers(table_key)
                    if not members:
                        return 0

                    # members are bytes; normalize to str
                    hashes = [
                        m.decode() if isinstance(m, (bytes, bytearray)) else str(m)
                        for m in members
                    ]
                    for qh in hashes:
                        try:
                            qkey = f"query:{qh}"
                            qmeta = f"query_meta:{qh}"
                            self.redis_client.delete(qkey)
                            self.redis_client.delete(qmeta)
                            removed_count += 1
                        except Exception as e:
                            self.log_and_trace(
                                f"Failed to remove redis cache for {qh}: {e}", "warning"
                            )

                    # Remove the table set itself
                    try:
                        self.redis_client.delete(table_key)
                    except Exception:
                        pass

                    self.log_and_trace(
                        f"Cleared {removed_count} redis cache entries for table: {table_name}"
                    )
                    return removed_count

                except Exception as e:
                    self.log_and_trace(
                        f"Error clearing redis cache by table '{table_name}': {e}",
                        "warning",
                    )
                    return 0

            with open(self.metadata_file, "r") as f:
                metadata = json.load(f)

            entries = metadata.get("cache_entries", {})
            to_remove = [
                qh
                for qh, entry in entries.items()
                if table_name in entry.get("tables", [])
            ]

            for qh in to_remove:
                entry = entries.get(qh)
                if not entry:
                    continue
                try:
                    path = Path(entry.get("file_path", ""))
                    if path.exists():
                        path.unlink()
                        removed_count += 1
                        self.log_and_trace(f"Removed cache file for query {qh}: {path}")
                except Exception as e:
                    self.log_and_trace(
                        f"Failed to remove cache file for {qh}: {e}", "warning"
                    )

                # Remove metadata entry
                entries.pop(qh, None)

            # Write updated metadata
            metadata["cache_entries"] = entries
            with open(self.metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)

        except Exception as e:
            self.log_and_trace(
                f"Error clearing cache by table '{table_name}': {e}", "warning"
            )

        self.log_and_trace(
            f"Cleared {removed_count} cache entries for table: {table_name}"
        )
        return removed_count
