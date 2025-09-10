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
SIMILARWEB_API_KEY = os.getenv("SIMILARWEB_API_KEY")  # Provided via Secret Manager
DOMAINS = [d.strip() for d in os.getenv("DOMAINS", "example.com").split(",") if d.strip()]
START_DATE = os.getenv("START_DATE", "2024-01")  # YYYY-MM
END_DATE = os.getenv("END_DATE", dt.date.today().strftime("%Y-%m"))
GRANULARITY = os.getenv("GRANULARITY", "monthly")
MAIN_DOMAIN_ONLY = os.getenv("MAIN_DOMAIN_ONLY", "true").lower() == "true"

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
    visits_series = data.get("visits", [])

    # Engagement metrics
    eng_base = f"https://api.similarweb.com/v1/website/{domain}/total-traffic-and-engagement/visits-duration"
    eng_params = params.copy()
    er = requests.get(eng_base, params=eng_params, timeout=60)
    engagement = {}
    if er.status_code == 200:
        ej = er.json()
        for key in ("avg_visit_duration", "pages_per_visit", "bounce_rate"):
            for row in ej.get(key, []):
                engagement.setdefault(row["date"], {}).update({key: row.get("value")})

    rows = []
    for row in visits_series:
        d = row.get("date")
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
