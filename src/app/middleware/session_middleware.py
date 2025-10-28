import time
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from opentelemetry.trace import Status, StatusCode
from ..db.duckdb_session import DuckDBSession
from ..telemetry.tracing import TracingMixin


class SessionMiddleware(BaseHTTPMiddleware, TracingMixin):
    def __init__(self, app):
        BaseHTTPMiddleware.__init__(self, app)
        TracingMixin.__init__(self)

    async def dispatch(self, request: Request, call_next):
        with self.tracer.start_as_current_span("session_middleware") as span:
            # Add request metadata
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.url", str(request.url))
            span.set_attribute("http.scheme", request.url.scheme)
            span.set_attribute("http.host", request.url.hostname or "unknown")
            span.set_attribute("http.path", request.url.path)

            if request.client:
                span.set_attribute("http.client.host", request.client.host)
                span.set_attribute("http.client.port", request.client.port)

            # Add user agent and other headers
            user_agent = request.headers.get("user-agent", "")
            if user_agent:
                span.set_attribute(
                    "http.user_agent", user_agent[:200]
                )  # Truncate long user agents

            content_type = request.headers.get("content-type", "")
            if content_type:
                span.set_attribute("http.request.content_type", content_type)

            request_id = f"req_{int(time.time() * 1000)}_{id(request)}"
            span.set_attribute("request.id", request_id)

            self.log_and_trace(
                f"Processing request {request_id}: {request.method} {request.url.path} "
                f"from {request.client.host if request.client else 'unknown'}"
            )

            # Create session with tracing
            session_start = time.time()

            with self.tracer.start_as_current_span(
                "create_duckdb_session"
            ) as session_span:
                session_creation_start = time.time()

                # Create a new DuckDB session for each request
                session = DuckDBSession(db_path=":memory:")
                request.state.duckdb_session = session

                session_creation_time = time.time() - session_creation_start
                session_span.set_attribute(
                    "session.creation_time_ms", session_creation_time * 1000
                )
                session_span.set_attribute("session.db_path", ":memory:")
                session_span.set_attribute("session.id", id(session))

                self.log_and_trace(
                    f"DuckDB session created in {session_creation_time:.3f}s for request {request_id}"
                )

            try:
                # Process the request
                with self.tracer.start_as_current_span(
                    "process_request"
                ) as process_span:
                    process_start = time.time()

                    response: Response = await call_next(request)

                    process_time = time.time() - process_start
                    process_span.set_attribute(
                        "request.processing_time_ms", process_time * 1000
                    )
                    process_span.set_attribute(
                        "response.status_code", response.status_code
                    )

                    # Add response headers info
                    content_length = response.headers.get("content-length")
                    if content_length:
                        process_span.set_attribute(
                            "response.content_length", int(content_length)
                        )

                    response_content_type = response.headers.get("content-type", "")
                    if response_content_type:
                        process_span.set_attribute(
                            "response.content_type", response_content_type
                        )

                    # Determine if request was successful
                    success = 200 <= response.status_code < 400
                    process_span.set_attribute("request.success", success)

                    if success:
                        process_span.set_status(Status(StatusCode.OK))
                        self.log_and_trace(
                            f"Request {request_id} processed successfully in {process_time:.3f}s "
                            f"(status: {response.status_code})"
                        )
                    else:
                        process_span.set_status(
                            Status(StatusCode.ERROR, f"HTTP {response.status_code}")
                        )
                        self.log_and_trace(
                            f"Request {request_id} completed with error in {process_time:.3f}s "
                            f"(status: {response.status_code})",
                            "warning",
                        )

                    return response

            except Exception as e:
                error_time = time.time() - session_start
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.set_attribute("error.type", type(e).__name__)
                span.set_attribute("error.message", str(e))
                span.set_attribute("error.time_ms", error_time * 1000)

                self.log_and_trace(
                    f"Request {request_id} failed after {error_time:.3f}s: {str(e)}",
                    "error",
                    error_type=type(e).__name__,
                )
                raise

            finally:
                # Clean up the session after the response is sent
                with self.tracer.start_as_current_span(
                    "cleanup_session"
                ) as cleanup_span:
                    cleanup_start = time.time()

                    try:
                        session.close_connection()
                        cleanup_time = time.time() - cleanup_start

                        cleanup_span.set_attribute(
                            "cleanup.time_ms", cleanup_time * 1000
                        )
                        cleanup_span.set_attribute("cleanup.success", True)

                        self.log_and_trace(
                            f"Session cleanup completed in {cleanup_time:.3f}s for request {request_id}"
                        )

                    except Exception as cleanup_error:
                        cleanup_time = time.time() - cleanup_start

                        cleanup_span.set_status(
                            Status(StatusCode.ERROR, str(cleanup_error))
                        )
                        cleanup_span.set_attribute(
                            "cleanup.time_ms", cleanup_time * 1000
                        )
                        cleanup_span.set_attribute("cleanup.success", False)
                        cleanup_span.set_attribute("cleanup.error", str(cleanup_error))

                        self.log_and_trace(
                            f"Session cleanup failed after {cleanup_time:.3f}s for request {request_id}: {str(cleanup_error)}",
                            "warning",
                        )

                total_time = time.time() - session_start
                span.set_attribute("middleware.total_time_ms", total_time * 1000)

                self.log_and_trace(
                    f"Request {request_id} middleware processing completed in {total_time:.3f}s"
                )
