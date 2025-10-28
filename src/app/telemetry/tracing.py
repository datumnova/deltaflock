import logging
import socket
import time
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.trace import Status, StatusCode
from ..config.settings import settings

logger = logging.getLogger(__name__)


def _test_otlp_connectivity(endpoint_url: str, timeout: int = 2) -> bool:
    """Test if OTLP endpoint is accessible"""
    try:
        # Parse URL to get host and port
        from urllib.parse import urlparse

        parsed = urlparse(endpoint_url)
        host = parsed.hostname or "localhost"
        # Default port should be 4318 for HTTP OTLP, not 4317 (gRPC)
        port = parsed.port or 4318

        # Test TCP connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()

        return result == 0
    except Exception as e:
        logger.debug(f"Connectivity test failed for {endpoint_url}: {e}")
        return False


def init_tracing(app):
    """Initialize OpenTelemetry tracing with fallback options"""

    # Check if tracing should be enabled
    enable_tracing = settings.telemetry_enabled

    if not enable_tracing:
        logger.info("OpenTelemetry tracing disabled")
        return

    try:
        # Enhanced resource attributes
        resource = Resource.create(
            {
                "service.name": settings.telemetry_service_name,
                "service.version": settings.telemetry_service_version,
                "service.instance.id": "local-instance",
                "deployment.environment": "development",
            }
        )

        trace.set_tracer_provider(TracerProvider(resource=resource))

        # Try to use OTLP exporter, fallback to console
        # Use HTTP endpoint for OTLP (port 4318) instead of gRPC (port 4317)
        otlp_http_endpoint = settings.telemetry_endpoint

        # Test connectivity before creating OTLP exporter
        if _test_otlp_connectivity(otlp_http_endpoint):
            try:
                otlp_exporter = OTLPSpanExporter(
                    endpoint=f"{otlp_http_endpoint}/v1/traces"
                )
                span_processor = BatchSpanProcessor(otlp_exporter)
                logger.info(f"Using OTLP HTTP exporter: {otlp_http_endpoint}")
            except Exception as e:
                logger.warning(
                    f"Failed to create OTLP exporter: {e}. Using console exporter"
                )
                console_exporter = ConsoleSpanExporter()
                span_processor = BatchSpanProcessor(console_exporter)
        else:
            logger.warning(
                f"OTLP endpoint {otlp_http_endpoint} not accessible. Using console exporter"
            )
            console_exporter = ConsoleSpanExporter()
            span_processor = BatchSpanProcessor(console_exporter)

        trace.get_tracer_provider().add_span_processor(span_processor)

        # Instrument FastAPI with detailed configuration
        FastAPIInstrumentor.instrument_app(
            app,
            excluded_urls="health,metrics",  # Exclude health check endpoints
            tracer_provider=trace.get_tracer_provider(),
        )

        # Instrument logging to correlate logs with traces
        LoggingInstrumentor().instrument(set_logging_format=True)

        logger.info("OpenTelemetry tracing initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize OpenTelemetry tracing: {e}")
        logger.info("Continuing without tracing")


class TracingMixin:
    """Mixin class to add tracing capabilities to any class"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracer = trace.get_tracer(self.__class__.__module__)
        self.logger = logging.getLogger(self.__class__.__module__)

    def create_span(self, operation_name: str, **attributes):
        """Create a new span with common attributes"""
        span = self.tracer.start_span(operation_name)

        # Add common attributes
        span.set_attribute("service.operation", operation_name)
        span.set_attribute("service.component", self.__class__.__name__)

        # Add custom attributes
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, str(value))

        return span

    def log_and_trace(self, message: str, level: str = "info", **extra_attrs):
        """Log a message and add it as a span event"""
        current_span = trace.get_current_span()

        # Log the message
        getattr(self.logger, level)(message, extra=extra_attrs)

        # Add as span event if we have an active span
        if current_span and current_span.is_recording():
            current_span.add_event(message, extra_attrs)


def trace_async_function(operation_name: str = None, **span_attributes):
    """Decorator to trace async functions with timing"""

    def decorator(func):
        import functools

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = trace.get_tracer(func.__module__)
            span_name = operation_name or f"{func.__module__}.{func.__name__}"

            with tracer.start_as_current_span(span_name) as span:
                # Add function metadata
                span.set_attribute("function.name", func.__name__)
                span.set_attribute("function.module", func.__module__)

                # Add custom attributes
                for key, value in span_attributes.items():
                    span.set_attribute(key, str(value))

                # Add arguments as attributes (be careful with sensitive data)
                if args:
                    span.set_attribute("function.args_count", len(args))
                if kwargs:
                    span.set_attribute("function.kwargs_count", len(kwargs))
                    # Log non-sensitive kwargs
                    safe_kwargs = {
                        k: v
                        for k, v in kwargs.items()
                        if not any(
                            sensitive in k.lower()
                            for sensitive in ["password", "token", "secret", "key"]
                        )
                    }
                    for k, v in safe_kwargs.items():
                        span.set_attribute(
                            f"function.arg.{k}", str(v)[:100]
                        )  # Truncate long values

                start_time = time.time()
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    raise
                finally:
                    duration = time.time() - start_time
                    span.set_attribute("function.duration_ms", duration * 1000)

        return wrapper

    return decorator


def trace_function(operation_name: str = None, **span_attributes):
    """Decorator to trace sync functions with timing"""

    def decorator(func):
        import functools

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = trace.get_tracer(func.__module__)
            span_name = operation_name or f"{func.__module__}.{func.__name__}"

            with tracer.start_as_current_span(span_name) as span:
                # Add function metadata
                span.set_attribute("function.name", func.__name__)
                span.set_attribute("function.module", func.__module__)

                # Add custom attributes
                for key, value in span_attributes.items():
                    span.set_attribute(key, str(value))

                start_time = time.time()
                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.set_attribute("error.type", type(e).__name__)
                    span.set_attribute("error.message", str(e))
                    raise
                finally:
                    duration = time.time() - start_time
                    span.set_attribute("function.duration_ms", duration * 1000)

        return wrapper

    return decorator
