# Full Cloud Run image: built-in datasets + Custom (requires datacommonsorg/data for import_validation only).
# Child birth testdata lives in this repo (sample_data/child_birth/). Sparse clone: only tools/import_validation.
# ------------------------------------------------------------------------------
# Stage 1: Sparse clone of data repo (only tools/import_validation for the runner)
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim AS data
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
ARG DATA_REPO_URL=https://github.com/datacommonsorg/data.git

# Sparse clone: only tools/import_validation (child_birth testdata is in this repo)
RUN git clone --depth 1 --filter=blob:none --sparse "${DATA_REPO_URL}" datacommonsorg/data \
    && cd datacommonsorg/data \
    && git sparse-checkout set tools/import_validation \
    && rm -rf .git

# ------------------------------------------------------------------------------
# Stage 2: Runtime image (slim base, no git)
# ------------------------------------------------------------------------------
FROM python:3.11-slim-bookworm

# Add metadata
LABEL maintainer="Data Commons Team" \
      version="1.0.0" \
      description="DC Import Validator - Full image with built-in datasets"

# Java 17 for dc-import JAR; curl for JAR download only (no git at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy only the sparse data subset from stage 1
COPY --from=data /app/datacommonsorg/data /app/datacommonsorg/data

# Copy this repo (dc-import-validator)
COPY . /app/dc-import-validator

WORKDIR /app/dc-import-validator

# Upgrade pip/wheel to reduce Python-related CVEs; then install app deps (import_validation + UI)
RUN pip install --no-cache-dir --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt -r ui/requirements.txt

# dc-import JAR with retry logic
ARG IMPORT_RELEASE_VERSION=v0.3.0
RUN mkdir -p bin && \
    echo "Downloading import tool JAR..." && \
    curl --retry 3 --retry-delay 5 -sL -o bin/datacommons-import-tool.jar \
    "https://github.com/datacommonsorg/import/releases/download/${IMPORT_RELEASE_VERSION}/datacommons-import-tool-0.3.0-jar-with-dependencies.jar" && \
    chmod 644 bin/datacommons-import-tool.jar && \
    echo "✓ JAR downloaded successfully"

# Verify JAR is valid (basic check)
RUN file bin/datacommons-import-tool.jar | grep -q "Zip archive" || \
    (echo "❌ JAR file appears corrupted" && exit 1)

# Create non-root user for security
RUN addgroup --system --gid 1001 app && \
    adduser --system --uid 1001 --gid 1001 app && \
    chown -R app:app /app

# Switch to non-root user
USER app

# run_e2e_test.sh expects PROJECTS_DIR so DATA_REPO = $PROJECTS_DIR/datacommonsorg/data
ENV PROJECTS_DIR=/app
EXPOSE 8080

# Cloud Run sets PORT; default 8080 for local
CMD ["sh", "-c", "uvicorn ui.server:app --host 0.0.0.0 --port ${PORT:-8080}"]