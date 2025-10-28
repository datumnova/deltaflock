import os
import time
import re
from duckdb import connect
from contextlib import contextmanager
from opentelemetry.trace import Status, StatusCode
from ..telemetry.tracing import TracingMixin, trace_function
from ..config.settings import settings


class DuckDBSession(TracingMixin):
    def __init__(self, db_path: str):
        super().__init__()
        self.db_path = db_path
        self.connection = None

    def _substitute_env_variables(self, sql_content: str) -> str:
        """
        Substitute environment variables in SQL content.
        Supports placeholders like ${VAR_NAME} or {VAR_NAME}
        """
        # Dictionary of environment variable mappings
        env_vars = {
            "UC_CATALOG_ENDPOINT": settings.uc_catalog_endpoint,
            "UC_CATALOG_TOKEN": settings.uc_catalog_token,
            "AZURE_STORAGE_ACCOUNT_NAME": settings.azure_storage_account_name,
            "AZURE_STORAGE_ACCOUNT_KEY": settings.azure_storage_account_key,
            "AZURE_ACCOUNT_NAME": settings.azure_account_name,
            "AZURE_CLIENT_ID": settings.azure_client_id,
            "AZURE_CLIENT_SECRET": settings.azure_client_secret,
            "AZURE_TENANT_ID": settings.azure_tenant_id,
        }

        # Substitute ${VAR_NAME} style placeholders
        def replace_var(match):
            var_name = match.group(1)
            if var_name in env_vars:
                value = env_vars[var_name]
                self.log_and_trace(
                    f"Substituting {var_name} with value: {value[:20]}..."
                )
                return value
            else:
                self.log_and_trace(
                    f"Warning: Environment variable {var_name} not found", "warning"
                )
                return match.group(0)  # Return original if not found

        # Replace both ${VAR_NAME} and {VAR_NAME} patterns
        sql_content = re.sub(r"\$\{([^}]+)\}", replace_var, sql_content)
        sql_content = re.sub(r"\{([A-Z_][A-Z0-9_]*)\}", replace_var, sql_content)

        return sql_content

    @trace_function("database_initialization")
    def _initialize_database(self):
        """Initialize the database with extensions and catalogs from configured SQL."""

        with self.tracer.start_as_current_span("db_initialization") as span:
            span.set_attribute("db.path", self.db_path)
            span.set_attribute("db.system", "duckdb")

            self.log_and_trace(
                f"Starting database initialization for path: {self.db_path}"
            )

            # Determine initialization SQL source (priority order):
            # 1. Custom SQL file from settings
            # 2. Default bundled SQL file
            init_sql = None
            init_source = "none"
            init_file_path = None

            if settings.duckdb_init_sql_file and os.path.exists(
                settings.duckdb_init_sql_file
            ):
                init_file_path = settings.duckdb_init_sql_file
                init_source = "custom_file"
                span.set_attribute("init.source", "custom_file")
                span.set_attribute("init.sql_path", init_file_path)
                self.log_and_trace(
                    f"Using initialization SQL from configured file: {init_file_path}"
                )

            else:
                # Fallback to bundled default SQL file
                current_dir = os.path.dirname(os.path.abspath(__file__))
                default_init_sql_path = os.path.join(current_dir, "duckdb_init.sql")

                if os.path.exists(default_init_sql_path):
                    init_file_path = default_init_sql_path
                    init_source = "bundled_file"
                    span.set_attribute("init.source", "bundled_file")
                    span.set_attribute("init.sql_path", init_file_path)
                    self.log_and_trace(
                        f"Using bundled initialization SQL file: {init_file_path}"
                    )
                else:
                    self.log_and_trace(
                        f"No bundled init SQL file found at {default_init_sql_path}",
                        "warning",
                    )

            # Read the SQL file if we have a valid path
            if init_file_path:
                try:
                    with open(init_file_path, "r") as f:
                        init_sql = f.read()
                except Exception as e:
                    self.log_and_trace(
                        f"Failed to read init SQL file {init_file_path}: {e}", "error"
                    )
                    init_sql = None

            span.set_attribute("init.source_type", init_source)
            span.set_attribute("init.sql_available", init_sql is not None)

            if init_sql:
                try:
                    with self.tracer.start_as_current_span(
                        "process_init_sql"
                    ) as read_span:
                        read_start = time.time()

                        # Substitute environment variables in the SQL
                        init_sql = self._substitute_env_variables(init_sql)

                        read_time = time.time() - read_start

                        read_span.set_attribute("sql.size_bytes", len(init_sql))
                        read_span.set_attribute("sql.process_time_ms", read_time * 1000)

                        self.log_and_trace(
                            f"Processed initialization SQL ({len(init_sql)} bytes) in {read_time:.3f}s"
                        )

                    with self.tracer.start_as_current_span(
                        "parse_sql_statements"
                    ) as parse_span:
                        parse_start = time.time()

                        # Remove comments and split SQL into statements more carefully
                        lines = init_sql.split("\n")
                        cleaned_lines = []
                        for line in lines:
                            if "--" in line:
                                line = line.split("--")[0]
                            cleaned_lines.append(line)

                        cleaned_sql = "\n".join(cleaned_lines)
                        statements = [
                            stmt.strip()
                            for stmt in cleaned_sql.split(";")
                            if stmt.strip()
                        ]

                        parse_time = time.time() - parse_start

                        parse_span.set_attribute("sql.statement_count", len(statements))
                        parse_span.set_attribute("sql.parse_time_ms", parse_time * 1000)

                        self.log_and_trace(
                            f"Parsed {len(statements)} SQL statements in {parse_time:.3f}s"
                        )

                    # Execute statements with individual tracing
                    successful_statements = 0
                    failed_statements = 0

                    for i, statement in enumerate(statements):
                        if statement:
                            with self.tracer.start_as_current_span(
                                f"execute_init_statement_{i + 1}"
                            ) as stmt_span:
                                stmt_span.set_attribute("sql.statement_number", i + 1)
                                stmt_span.set_attribute(
                                    "sql.statement_preview", statement[:100]
                                )
                                stmt_span.set_attribute(
                                    "sql.statement_length", len(statement)
                                )

                                try:
                                    stmt_start = time.time()
                                    self.connection.execute(statement)
                                    stmt_time = time.time() - stmt_start

                                    stmt_span.set_attribute(
                                        "sql.execution_time_ms", stmt_time * 1000
                                    )
                                    stmt_span.set_status(Status(StatusCode.OK))
                                    successful_statements += 1

                                    self.log_and_trace(
                                        f"Successfully executed statement {i + 1}/{len(statements)} "
                                        f"in {stmt_time:.3f}s: {statement[:50]}..."
                                    )

                                except Exception as e:
                                    stmt_time = time.time() - stmt_start
                                    stmt_span.set_status(
                                        Status(StatusCode.ERROR, str(e))
                                    )
                                    stmt_span.set_attribute("error.message", str(e))
                                    stmt_span.set_attribute(
                                        "sql.execution_time_ms", stmt_time * 1000
                                    )
                                    failed_statements += 1

                                    self.log_and_trace(
                                        f"Failed to execute statement {i + 1}/{len(statements)}: {statement[:50]}... "
                                        f"Error: {str(e)}",
                                        "warning",
                                    )
                                    continue

                    span.set_attribute(
                        "init.successful_statements", successful_statements
                    )
                    span.set_attribute("init.failed_statements", failed_statements)
                    span.set_attribute(
                        "init.success_rate",
                        successful_statements / len(statements) if statements else 0,
                    )

                    self.log_and_trace(
                        f"Database initialization completed: {successful_statements}/{len(statements)} statements successful"
                    )

                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    self.log_and_trace(
                        f"Failed to process initialization SQL: {e}", "error"
                    )
                    self._fallback_initialization()
            else:
                span.set_attribute("init.fallback_used", True)
                self.log_and_trace(
                    "No initialization SQL available from any source, using fallback",
                    "warning",
                )
                self._fallback_initialization()

    @trace_function("fallback_initialization")
    def _fallback_initialization(self):
        """Fallback initialization if SQL file fails."""
        with self.tracer.start_as_current_span("fallback_initialization") as span:
            self.log_and_trace("Starting fallback database initialization")

            fallback_commands = [
                "INSTALL uc_catalog",
                "INSTALL delta",
                "LOAD delta",
                "LOAD uc_catalog",
            ]

            successful_commands = 0

            for i, command in enumerate(fallback_commands):
                with self.tracer.start_as_current_span(
                    f"fallback_command_{i + 1}"
                ) as cmd_span:
                    cmd_span.set_attribute("command", command)

                    try:
                        cmd_start = time.time()
                        self.connection.execute(command)
                        cmd_time = time.time() - cmd_start

                        cmd_span.set_attribute("execution_time_ms", cmd_time * 1000)
                        cmd_span.set_status(Status(StatusCode.OK))
                        successful_commands += 1

                        self.log_and_trace(
                            f"Fallback command executed successfully in {cmd_time:.3f}s: {command}"
                        )

                    except Exception as e:
                        cmd_span.set_status(Status(StatusCode.ERROR, str(e)))
                        self.log_and_trace(
                            f"Fallback command failed: {command} - Error: {str(e)}",
                            "error",
                        )

            span.set_attribute("successful_commands", successful_commands)
            span.set_attribute("total_commands", len(fallback_commands))

            if successful_commands == 0:
                span.set_status(
                    Status(StatusCode.ERROR, "All fallback commands failed")
                )
                self.log_and_trace(
                    "All fallback initialization commands failed", "error"
                )
            else:
                self.log_and_trace(
                    f"Fallback initialization completed: {successful_commands}/{len(fallback_commands)} commands successful"
                )

    @contextmanager
    def get_connection(self):
        with self.tracer.start_as_current_span("database_session") as span:
            span.set_attribute("db.path", self.db_path)
            span.set_attribute("connection.new", self.connection is None)

            connection_start = time.time()

            if self.connection is None:
                with self.tracer.start_as_current_span(
                    "create_connection"
                ) as conn_span:
                    create_start = time.time()

                    self.log_and_trace(
                        f"Creating new DuckDB connection to: {self.db_path}"
                    )
                    self.connection = connect(self.db_path)

                    create_time = time.time() - create_start
                    conn_span.set_attribute(
                        "connection.create_time_ms", create_time * 1000
                    )

                    self.log_and_trace(
                        f"DuckDB connection created in {create_time:.3f}s"
                    )

                    # Initialize database
                    self._initialize_database()

            try:
                connection_ready_time = time.time() - connection_start
                span.set_attribute(
                    "connection.ready_time_ms", connection_ready_time * 1000
                )

                self.log_and_trace(
                    f"Database connection ready in {connection_ready_time:.3f}s"
                )
                yield self.connection

            finally:
                cleanup_start = time.time()
                self.close_connection()
                cleanup_time = time.time() - cleanup_start

                span.set_attribute("connection.cleanup_time_ms", cleanup_time * 1000)
                self.log_and_trace(
                    f"Database connection cleaned up in {cleanup_time:.3f}s"
                )

    @trace_function("close_connection")
    def close_connection(self):
        if self.connection:
            with self.tracer.start_as_current_span("close_db_connection") as span:
                close_start = time.time()

                self.log_and_trace("Closing DuckDB connection")
                self.connection.close()
                self.connection = None

                close_time = time.time() - close_start
                span.set_attribute("close_time_ms", close_time * 1000)

                self.log_and_trace(f"DuckDB connection closed in {close_time:.3f}s")
