# Repository layout
# .
# ├── app/
# │   └── main.py
# ├── requirements.txt
# ├── Dockerfile
# └── deploy.sh

# =========================
# app/main.py
# =========================
cat > app/main.py << 'PY'
import os
import json
import datetime as dt
from typing import List, Dict

import requests
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery

app = FastAPI()

PROJECT_ID = os.getenv("PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "marketing")
BQ_TABLE = os.getenv("BQ_TABLE", "similarweb_traffic")
SIMILARWEB_API_KEY = os.getenv("SIMILARWEB_API_KEY")  # Recommended: provide via Secret Manager as env var
DOMAINS = [d.strip() for d in os.getenv("DOMAINS", "example.com").split(",") if d.strip()]
START_DATE = os.getenv("START_DATE", "2024-01")  # YYYY-MM
END_DATE = os.getenv("END_DATE", dt.date.today().strftime("%Y-%m"))
GRANULARITY = os.getenv("GRANULARITY", "monthly")  # monthly|daily depending on endpoint
MAIN_DOMAIN_ONLY = os.getenv("MAIN_DOMAIN_ONLY", "true").lower() == "true"

# Build a BigQuery client once per instance
bq_client = bigquery.Client(project=PROJECT_ID)

def ensure_table():
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, BQ_DATASET)
    table_ref = dataset_ref.table(BQ_TABLE)

    schema = [
        bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("visits", "FLOAT"),
        bigquery.SchemaField("avg_visit_duration", "FLOAT"),
        bigquery.SchemaField("pages_per_visit", "FLOAT"),
        bigquery.SchemaField("bounce_rate", "FLOAT"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    try:
        bq_client.get_table(table_ref)
    except Exception:
        bq_client.create_dataset(dataset_ref, exists_ok=True)
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="date")
        table.clustering_fields = ["domain"]
        bq_client.create_table(table, exists_ok=True)


def fetch_visits(domain: str) -> List[Dict]:
    """Fetch traffic + engagement for a domain from Similarweb API."""
    base = f"https://api.similarweb.com/v1/website/{domain}/total-traffic-and-engagement/visits"
    params = {
        "start_date": START_DATE,
        "end_date": END_DATE,
        "granularity": GRANULARITY,
        "main_domain_only": str(MAIN_DOMAIN_ONLY).lower(),
        "api_key": SIMILARWEB_API_KEY,
    }
    r = requests.get(base, params=params, timeout=60)
    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Similarweb error for {domain}: {r.status_code} {r.text}")
    data = r.json()
    # Expected shape: {"visits": [{"date": "2024-01-01", "visits": 123.0}, ...]}
    visits_series = data.get("visits", [])

    # Additional engagement endpoint
    eng_base = f"https://api.similarweb.com/v1/website/{domain}/total-traffic-and-engagement/visits-duration"
    eng_params = {
        "start_date": START_DATE,
        "end_date": END_DATE,
        "granularity": GRANULARITY,
        "main_domain_only": str(MAIN_DOMAIN_ONLY).lower(),
        "api_key": SIMILARWEB_API_KEY,
    }
    er = requests.get(eng_base, params=eng_params, timeout=60)
    engagement = {}
    if er.status_code == 200:
        ej = er.json()
        # Expected: {"avg_visit_duration": [{"date": "2024-01-01", "value": 123.4}], "pages_per_visit": [...], "bounce_rate": [...]} (actual endpoints may vary)
        for key in ("avg_visit_duration", "pages_per_visit", "bounce_rate"):
            for row in ej.get(key, []):
                engagement.setdefault(row["date"], {}).update({key: row.get("value")})

    rows = []
    for row in visits_series:
        d = row.get("date")  # e.g. 2024-01-01
        e = engagement.get(d, {})
        rows.append({
            "domain": domain,
            "date": d,
            "visits": row.get("visits"),
            "avg_visit_duration": e.get("avg_visit_duration"),
            "pages_per_visit": e.get("pages_per_visit"),
            "bounce_rate": e.get("bounce_rate"),
            "source": "similarweb",
            "ingested_at": dt.datetime.utcnow().isoformat(),
        })
    return rows


def load_rows(rows: List[Dict]):
    if not rows:
        return
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    errors = bq_client.insert_rows_json(table_id, rows)
    if errors:
        raise HTTPException(status_code=500, detail=str(errors))


@app.get("/")
async def root():
    return {"status": "ok", "domains": DOMAINS, "dataset": f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"}


@app.post("/run")
async def run():
    if not SIMILARWEB_API_KEY:
        raise HTTPException(status_code=500, detail="SIMILARWEB_API_KEY is not set")
    ensure_table()
    all_rows = []
    for domain in DOMAINS:
        batch = fetch_visits(domain)
        all_rows.extend(batch)
    load_rows(all_rows)
    return {"inserted": len(all_rows)}
PY

# =========================
# requirements.txt
# =========================
cat > requirements.txt << 'REQ'
fastapi==0.115.0
uvicorn[standard]==0.30.6
google-cloud-bigquery==3.25.0
requests==2.32.3
REQ

# =========================
# Dockerfile
# =========================
cat > Dockerfile << 'DOCKER'
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app

# Cloud Run expects the service to listen on $PORT
ENV PORT=8080
CMD exec uvicorn app.main:app --host 0.0.0.0 --port $PORT
DOCKER

# =========================
# deploy.sh (helper commands)
# =========================
cat > deploy.sh << 'SH'
#!/usr/bin/env bash
set -euo pipefail

# REQUIRED: set these before running
PROJECT_ID=${PROJECT_ID:-"your-gcp-project"}
REGION=${REGION:-"europe-west2"} # London
SERVICE=${SERVICE:-"similarweb-to-bq"}

# Config you can customize
BQ_DATASET=${BQ_DATASET:-"marketing"}
BQ_TABLE=${BQ_TABLE:-"similarweb_traffic"}
DOMAINS=${DOMAINS:-"example.com,anotherdomain.com"}
START_DATE=${START_DATE:-"2024-01"}
END_DATE=${END_DATE:-""} # empty => defaults in app

# Enable services
gcloud services enable run.googleapis.com secretmanager.googleapis.com bigquery.googleapis.com cloudbuild.googleapis.com --project "$PROJECT_ID"

# Create dataset if needed
bq --project_id="$PROJECT_ID" --location=EU mk -d --description "Marketing analytics" "$BQ_DATASET" || true

# Build & deploy
gcloud builds submit --project "$PROJECT_ID" --tag "gcr.io/$PROJECT_ID/$SERVICE"

# Create (or update) a secret for the Similarweb API key before deploy:
#   echo -n "YOUR_API_KEY" | gcloud secrets create similarweb-api-key --data-file=- --replication-policy=automatic
# or update existing secret:
#   echo -n "YOUR_API_KEY" | gcloud secrets versions add similarweb-api-key --data-file=-

# Deploy to Cloud Run with env vars and secret
gcloud run deploy "$SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --image "gcr.io/$PROJECT_ID/$SERVICE" \
  --platform managed \
  --allow-unauthenticated=false \
  --set-env-vars "PROJECT_ID=$PROJECT_ID,BQ_DATASET=$BQ_DATASET,BQ_TABLE=$BQ_TABLE,DOMAINS=$DOMAINS,START_DATE=$START_DATE" \
  --set-secrets "SIMILARWEB_API_KEY=similarweb-api-key:latest"

# Create a service account to invoke the service from Cloud Scheduler
INVOKER_SA="${SERVICE}-invoker@$PROJECT_ID.iam.gserviceaccount.com"
@if ! gcloud iam service-accounts describe "$INVOKER_SA" --project "$PROJECT_ID" >/dev/null 2>&1; then
  gcloud iam service-accounts create "${SERVICE}-invoker" --project "$PROJECT_ID" --display-name "Cloud Scheduler Invoker for $SERVICE"
fi

# Grant run.invoker to the SA
SERVICE_URL=$(gcloud run services describe "$SERVICE" --project "$PROJECT_ID" --region "$REGION" --format 'value(status.url)')
POLICY_BINDING="serviceAccount:$INVOKER_SA"
gcloud run services add-iam-policy-binding "$SERVICE" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --member "$POLICY_BINDING" \
  --role "roles/run.invoker"

echo "Deployed to: $SERVICE_URL"
echo "To schedule daily run at 08:00 London time:"
echo "gcloud scheduler jobs create http similarweb-pull --project $PROJECT_ID --location $REGION --schedule '0 8 * * *' --http-method POST --uri ${SERVICE_URL}/run --oidc-service-account-email ${INVOKER_SA}"
SH

chmod +x deploy.sh
