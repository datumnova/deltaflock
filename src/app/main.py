import logging
import time
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from opentelemetry import trace
from .api.v1.routes import router as api_router
from .middleware.session_middleware import SessionMiddleware
from .telemetry.tracing import init_tracing, trace_async_function
import os
import subprocess
import sys
from pathlib import Path

# Configure logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Golduck - DuckDB Delta API",
    icon="🦆",
    description="A high-performance API for querying DuckDB with Delta Lake support and comprehensive OpenTelemetry tracing",
    version="1.0.0",
)
os.environ["AZURE_LOG_LEVEL"] = "verbose"

# Setup OpenTelemetry tracing (gracefully handles failures)
try:
    init_tracing(app)
    logger.info("OpenTelemetry tracing initialized successfully")
except Exception as e:
    logger.warning(f"Failed to initialize tracing: {e}")
    logger.info("Application will continue without tracing")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include session middleware
app.add_middleware(SessionMiddleware)

# Include API routes
app.include_router(api_router, prefix="/api/v1")

# Get tracer for main app
tracer = trace.get_tracer(__name__)


@app.get("/", tags=["Health"])
@trace_async_function("root_endpoint")
async def root():
    """Root endpoint with basic API information"""
    with tracer.start_as_current_span("root_handler") as span:
        span.set_attribute("endpoint.name", "root")
        span.set_attribute("response.type", "welcome")

        logger.info("Root endpoint accessed")

        return {
            "message": "Welcome to the DuckDB Delta API!",
            "version": "1.0.0",
            "docs": "/docs",
            "health": "/health",
            "api": "/api/v1/query",
        }


@app.get("/health", tags=["Health"])
@trace_async_function("health_check")
async def health_check():
    """Health check endpoint with system status"""
    with tracer.start_as_current_span("health_check_handler") as span:
        start_time = time.time()

        span.set_attribute("endpoint.name", "health_check")
        span.set_attribute("check.type", "basic")

        try:
            # Basic health checks
            import psutil
            import sys

            # System metrics
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()

            health_data = {
                "status": "healthy",
                "timestamp": time.time(),
                "uptime_seconds": time.time() - start_time,
                "system": {
                    "cpu_percent": cpu_percent,
                    "memory_percent": memory.percent,
                    "memory_available_mb": memory.available / (1024 * 1024),
                    "python_version": sys.version,
                },
                "services": {
                    "api": "operational",
                    "tracing": "enabled" if trace.get_tracer_provider() else "disabled",
                },
            }

            # Add metrics to span
            span.set_attribute("health.status", "healthy")
            span.set_attribute("system.cpu_percent", cpu_percent)
            span.set_attribute("system.memory_percent", memory.percent)
            span.set_attribute(
                "system.memory_available_mb", memory.available / (1024 * 1024)
            )

            logger.info(
                f"Health check completed successfully - CPU: {cpu_percent}%, Memory: {memory.percent}%"
            )

            return health_data

        except ImportError:
            # Fallback if psutil is not available
            import sys

            health_data = {
                "status": "healthy",
                "timestamp": time.time(),
                "system": {"python_version": sys.version},
                "services": {
                    "api": "operational",
                    "tracing": "enabled" if trace.get_tracer_provider() else "disabled",
                },
                "note": "Limited system metrics (psutil not available)",
            }

            span.set_attribute("health.status", "healthy")
            span.set_attribute("health.limited_metrics", True)

            logger.info("Health check completed with limited metrics")

            return health_data

        except Exception as e:
            span.set_attribute("health.status", "unhealthy")
            span.set_attribute("health.error", str(e))

            logger.error(f"Health check failed: {str(e)}")

            return {"status": "unhealthy", "timestamp": time.time(), "error": str(e)}


if __name__ == "__main__":
    import uvicorn

    logger.info("Starting DuckDB Delta API server...")
    uvicorn.run(app, host="0.0.0.0", port=9000)


def main() -> int:
    """Callable entrypoint for programmatic/packaged use.

    This allows console scripts or PEP-517 build tools to import a callable
    `main` from the `app` package and start the server (used by the `psyduck` entrypoint).
    Returns 0 on clean exit.
    """
    import uvicorn

    def _print_colored_art():
        """Attempt to run the renderer script to print colored HTML art.

        Falls back to plain ASCII art if script or HTML not available.
        """
        # bundled html (next to this file)
        html_path = Path(__file__).resolve().parent / "static/ascii_art.html"
        # script path in repo root scripts/
        repo_root = Path(__file__).resolve().parents[2]
        script_path = repo_root / "scripts" / "render_ascii.py"

        if script_path.exists() and html_path.exists():
            try:
                # Use the same python executable to run the renderer
                subprocess.run(
                    [sys.executable, str(script_path), str(html_path)], check=False
                )
                return
            except Exception as e:
                logger.warning(f"Failed to run renderer script: {e}")

    logger.info("Golduck engaged — ready to quack queries!")
    logger.info("Starting DuckDB Delta API server via main()...")
    # Print colored art (or fallback)
    _print_colored_art()
    uvicorn.run(app, host="0.0.0.0", port=9000)
    return 0
