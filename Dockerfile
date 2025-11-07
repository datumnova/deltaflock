# Use Python 3.12 slim image as base
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies and uv
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/* \
    && pip install uv

# Copy project files
COPY pyproject.toml .
COPY uv.lock .
COPY README.md .

# Copy source code
COPY src/ ./src/
COPY scripts/ ./scripts/

# Create necessary directories and install dependencies
RUN mkdir -p query_cache && \
    uv sync --frozen

# Expose the port that deltaflock runs on
EXPOSE 9000

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9000/health || exit 1

# Run the application
CMD ["uv", "run", "deltaflock"]