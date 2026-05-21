# Full Cloud Run / Cloud Batch image: built-in datasets + Custom uploads.
# Child birth testdata lives in this repo (sample_data/child_birth/).
# Sparse clone from datacommonsorg/data: tools/import_validation, tools/import_differ, util, tools/statvar_importer.
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

# Sparse clone: tools/import_validation (runner) + tools/import_differ (differ step)
# Also includes util/ and tools/statvar_importer/ because validator.py (as of upstream commit
# c278165c) unconditionally imports util/counters.py and tools/import_validation/validator_goldens.py,
# which in turn imports tools/statvar_importer/{mcf_diff,data_sampler,mcf_file_util}.py and util/file_util.py
# at module load time. These imports happen even when the GOLDENS_CHECK validator is not used.
RUN git clone --depth 1 --filter=blob:none --sparse "${DATA_REPO_URL}" datacommonsorg/data \
    && cd datacommonsorg/data \
    && git sparse-checkout set tools/import_validation tools/import_differ util tools/statvar_importer \
    && rm -rf .git

# ------------------------------------------------------------------------------
# Stage 2: Runtime image (slim base, no git)
# ------------------------------------------------------------------------------
FROM python:3.11-slim-bookworm

# Add metadata
LABEL maintainer="Data Commons Team" \
      version="0.4.0" \
      description="DC Import Validator - Full image with built-in datasets"

# Java 17 for dc-import JAR; curl for JAR download
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

# dc-import JAR (-f = fail on HTTP errors; retry on transient failure)
ARG IMPORT_RELEASE_VERSION=v0.3.0
RUN IMPORT_VERSION_NUM="${IMPORT_RELEASE_VERSION#v}" && \
    mkdir -p bin && \
    curl --retry 3 --retry-delay 5 -sfL -o bin/datacommons-import-tool.jar \
    "https://github.com/datacommonsorg/import/releases/download/${IMPORT_RELEASE_VERSION}/datacommons-import-tool-${IMPORT_VERSION_NUM}-jar-with-dependencies.jar" && \
    chmod 644 bin/datacommons-import-tool.jar

# Create non-root user for security
RUN addgroup --system --gid 1001 app && \
    adduser --system --uid 1001 --gid 1001 app && \
    chown -R app:app /app

# Switch to non-root user
USER app

# Build metadata — baked in at image build time, readable at runtime by batch/entrypoint.sh
# and the FastAPI server startup. Pass via: --build-arg GIT_SHA=$(git rev-parse HEAD)
#                                           --build-arg BUILD_DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)
ARG GIT_SHA=unknown
ARG BUILD_DATE=unknown
ENV GIT_SHA=$GIT_SHA
ENV BUILD_DATE=$BUILD_DATE

# run_e2e_test.sh uses DATA_REPO when set, else defaults to $PROJECTS_DIR/datacommonsorg/data
ENV PROJECTS_DIR=/app
# Avoid "Fontconfig error: No writable cache directories" in Cloud Run (read-only fs except /tmp).
# Java and other libs use fontconfig; point cache to writable /tmp so logs stay clean.
ENV XDG_CACHE_HOME=/tmp/.cache
EXPOSE 8080

# Cloud Run sets PORT; default 8080 for local
CMD ["sh", "-c", "uvicorn ui.server:app --host 0.0.0.0 --port ${PORT:-8080}"]