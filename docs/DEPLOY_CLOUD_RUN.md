# Deploying DC Import Validator on Google Cloud Run

This guide walks through deploying the DC Import Validator UI on **Google Cloud Run** so your team can access it without running the app locally.

---

## Quick Decision Guide

| If you... | Start here |
|-----------|------------|
| Want to test first | [Test locally](#test-the-image-locally-before-cloud-run) ↓ |
| Prefer click-based setup | [Cloud Console UI](#build--deploy-cli-vs-console-ui) |
| Love the command line | [gcloud CLI](#using-the-cloud-run-cli) |
| Want automated deploys | [GitHub Actions](#deploy-via-github-actions-cicd) |

---

## First Step: Choose Your Option

### Option A: Custom-only (Simpler, Smaller Image)
- :white_check_mark: Only "Custom upload" works
- :white_check_mark: Smaller image (~1GB)
- :white_check_mark: Faster builds
- :x: No built-in test datasets

### Option B: Full (Built-in datasets + Custom)
- :white_check_mark: All datasets work (child_birth, fail cases, AI demo)
- :white_check_mark: Complete testing experience
- :x: Larger image (~2.4GB)
- :x: Longer builds

**Recommendation:** Start with Option A. Add Option B later if the team needs built-in datasets in Cloud Run.

---

## Test the Image Locally (Before Cloud Run)

Always test locally first — it's faster and free!

```bash
# 1. Build (from repo root)
cd /path/to/dc-import-validator
docker build -t dc-import-validator .

# For Apple Silicon (M1/M2/M3):
docker build --platform linux/amd64 -t dc-import-validator .

# 2. Run
docker run --rm -p 8080:8080 dc-import-validator

# With Gemini API key:
docker run --rm -p 8080:8080 -e GEMINI_API_KEY=your_key_here dc-import-validator
```

**Test in browser:** Open http://localhost:8080  

**Stop:** Ctrl+C in terminal

:white_check_mark: If it works locally, it'll work on Cloud Run!

---

## Build & Deploy: CLI vs Console UI

| | gcloud CLI | Cloud Console UI |
|--|------------|------------------|
| **Best for** | Automation, CI/CD, repeatable deployments | One-off testing, first-time setup |
| **Pros** | Scriptable, copy-paste friendly, version-controlled | Visual, no CLI install, see all options |
| **Cons** | Need gcloud installed | Manual steps each time |

**Recommendation:** Test locally with Docker first. Then:

- Use **Console UI** for your first deploy to explore options.
- Switch to **gcloud CLI** or **GitHub Actions** for repeatable deployments.

---

## What You'll Need

### 1. Container Image

Dockerfile includes:

- Python 3 + Java 17
- App code + dependencies
- Import tool JAR (downloaded at build time)
- Data repo (optional, depending on Option A/B)

### 2. Port Configuration

Cloud Run sets `$PORT` (default 8080). Your app must use it:

```bash
uvicorn ui.server:app --host 0.0.0.0 --port ${PORT:-8080}
```

### 3. Secrets & Environment

| Variable | Purpose | Where to store |
|----------|---------|----------------|
| GEMINI_API_KEY | AI Review | Secret Manager (recommended) |
| DC_API_KEY | Java import tool (FULL + existence checks) | Secret Manager (required when using FULL) |
| GCS_REPORTS_BUCKET | Report storage | Environment variable |
| LOG_LEVEL | Debugging | Environment variable |
| VALIDATION_RUN_TIMEOUT_SEC | Run time limit | Environment variable |

If `IMPORT_RESOLUTION_MODE=FULL` and `IMPORT_EXISTENCE_CHECKS=true`, then **DC_API_KEY must be configured** as a Secret in Cloud Run (the Java import tool uses it for Recon and existence-check API calls). Example:

```bash
--set-secrets="DC_API_KEY=dc-api-key:latest"
```

### 4. Resource Limits

| Resource | Recommended | Why |
|----------|-------------|-----|
| Memory | ≥ 2 GiB | Java genmcf + Python validation |
| CPU | ≥ 2 | Parallel processing |
| Timeout | 10–60 min | Depends on CSV size |

### 5. Stateless Design

- Instances scale to zero.
- Uploads are ephemeral.
- Set `GCS_REPORTS_BUCKET` for persistent reports.

---

## Data Repo Dependency (Important!)

The pipeline needs `datacommonsorg/data` for import_validation. The E2E script uses `DATA_REPO` when set (e.g. in Docker/Cloud Run); otherwise it defaults to `../datacommonsorg/data` relative to the script. Here's how to handle it:

**Option A: Custom-only (Recommended)**

```dockerfile
# No data repo needed! Just refactor to call runner directly
# OR include minimal slice:
RUN git clone --depth 1 --filter=blob:none --sparse \
    https://github.com/datacommonsorg/data.git && \
    cd data && git sparse-checkout set tools/import_validation
```

**Option B: Full (Built-in datasets)**

```dockerfile
# Include full data repo (or relevant subset)
RUN git clone https://github.com/datacommonsorg/data.git /app/data
ENV DATA_REPO=/app/data
```

---

## Deployment Checklist

### Pre-deploy

- [ ] Choose Option A or B
- [ ] Dockerfile builds locally
- [ ] App runs at http://localhost:8080
- [ ] Test validation works (try a dataset)

### Cloud Run configuration

- [ ] Memory: ≥ 2Gi
- [ ] Timeout: ≥ 10 min
- [ ] Port: 8080 (uses `$PORT`)
- [ ] Environment variables set
- [ ] Secrets configured (Gemini key)

### GCS Reports (optional but recommended)

- [ ] Created bucket: `dc-import-validator-reports`
- [ ] Granted Storage Object Admin to Cloud Run service account
- [ ] Set `GCS_REPORTS_BUCKET` env var

---

## Deploy with gcloud CLI

```bash
# 1. Build and push
gcloud builds submit --tag gcr.io/YOUR_PROJECT_ID/dc-import-validator .

# Or to Artifact Registry:
gcloud builds submit --tag REGION-docker.pkg.dev/YOUR_PROJECT_ID/REPO/dc-import-validator .

# 2. Deploy
gcloud run deploy dc-import-validator \
  --image gcr.io/YOUR_PROJECT_ID/dc-import-validator \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "GCS_REPORTS_BUCKET=dc-import-validator-reports,IMPORT_RESOLUTION_MODE=FULL,IMPORT_EXISTENCE_CHECKS=true" \
  --set-secrets="GEMINI_API_KEY=gemini-key:latest,DC_API_KEY=dc-api-key:latest" \
  --memory 2Gi \
  --timeout 600
```

---

## Deploy with GitHub Actions (CI/CD)

### One-time setup

**1. Create Artifact Registry repo**

```bash
gcloud artifacts repositories create dc-import-validator \
  --repository-format=docker \
  --location=us-central1
```

**2. Create service account**

```bash
# Create SA
gcloud iam service-accounts create github-actions-deploy

# Grant permissions
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:github-actions-deploy@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"

gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
  --member="serviceAccount:github-actions-deploy@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/run.admin"

# Create and download key
gcloud iam service-accounts keys create key.json \
  --iam-account=github-actions-deploy@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

**3. Add GitHub secrets**

| Secret | Value |
|--------|-------|
| GCP_PROJECT_ID | Your GCP project ID |
| GCP_SA_KEY | Contents of `key.json` |
| AR_LOCATION (optional) | e.g. `us-central1` |

### That's it!

Push to `main` → automatic build + deploy :rocket:

---

## Troubleshooting

| Problem | Likely fix |
|---------|------------|
| Validation fails with path errors | Data repo not found → Check Option A/B setup |
| Gemini Review not working | API key missing → Check secrets |
| Timeouts | Increase Cloud Run timeout |
| Memory errors | Increase memory to 4Gi |
| Reports not showing | Check GCS bucket permissions |

---

## Next Steps

- :white_check_mark: Test locally with Docker
- :white_check_mark: Choose Option A or B
- :white_check_mark: Deploy to Cloud Run
- :black_square_button: Set up GitHub Actions
- :black_square_button: Share the URL with your team!

Need help? Open an issue or ping the team! :tada: