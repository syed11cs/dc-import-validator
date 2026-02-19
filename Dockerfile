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

# Python deps (covers import_validation + UI)
RUN pip install --no-cache-dir -r requirements.txt -r ui/requirements.txt

# dc-import JAR (same version as setup.sh)
ARG IMPORT_RELEASE_VERSION=v0.3.0
RUN mkdir -p bin && curl -sL -o bin/datacommons-import-tool.jar \
    "https://github.com/datacommonsorg/import/releases/download/${IMPORT_RELEASE_VERSION}/datacommons-import-tool-0.3.0-jar-with-dependencies.jar"

# run_e2e_test.sh expects PROJECTS_DIR so DATA_REPO = $PROJECTS_DIR/datacommonsorg/data
ENV PROJECTS_DIR=/app
EXPOSE 8080

# Cloud Run sets PORT; default 8080 for local
CMD ["sh", "-c", "uvicorn ui.server:app --host 0.0.0.0 --port ${PORT:-8080}"]
