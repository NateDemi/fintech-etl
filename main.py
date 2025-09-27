from __future__ import annotations
import json
import logging
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Header, BackgroundTasks, Body, Depends
from google.cloud import storage
from pydantic_settings import BaseSettings
from pydantic import field_validator, Field

# Core processing utilities (your simplified module)
from stream.util import WebhookClient, process_csv_from_bytes, process_csv_from_gcs

# Your existing intake handler (keeps request parsing/upload policy in one place)
from intake.handlers import ingest_csv_handler

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("fintech-etl")
logger.info("🚀 Starting Fintech ETL Service")

# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------
class Settings(BaseSettings):
    gcs_bucket: str = Field(alias="GCS_BUCKET")  # e.g., "fintech-inbox"
    webhook_url: Optional[str] = Field(default=None, alias="WEBHOOK_URL")
    webhook_headers: dict = Field(default={}, alias="WEBHOOK_HEADERS")
    intake_token: Optional[str] = Field(default=None, alias="INTAKE_TOKEN")  # optional bearer for /ingest (recommended in prod)

    @field_validator("webhook_headers", mode="before")
    @classmethod
    def _parse_headers(cls, v):
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                logging.warning("WEBHOOK_HEADERS not valid JSON; ignoring")
                return {}
        return v

    class Config:
        env_file = ".env"
        extra = "ignore"

settings = Settings()

logger.info("📦 GCS Bucket: %s", settings.gcs_bucket)
logger.info("🔗 Webhook URL: %s", (settings.webhook_url[:64] + "…") if settings.webhook_url else "Not configured")

# Single webhook client instance
webhook_client = WebhookClient(settings.webhook_url, settings.webhook_headers)

# FastAPI app
app = FastAPI(title="Fintech ETL Service", version="1.0.0")


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def get_storage_client() -> storage.Client:
    try:
        return storage.Client()
    except Exception as e:
        logger.exception("Failed to initialize GCS client")
        raise HTTPException(status_code=500, detail=f"GCS client init failed: {e}")


# -----------------------------------------------------------------------------
# Intake (Apps Script -> Cloud Run)
# -----------------------------------------------------------------------------
@app.post("/ingest")
async def ingest_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    gmail_id: str = Form(...),
    received_date: str = Form(...),
    original_name: str = Form(...),
    google_drive_url: str | None = Form(None),
    authorization: str | None = Header(default=None),
):
    """
    Receive a CSV attachment and process directly, in-memory.
    Delegates upload/token checks to your existing ingest_csv_handler,
    then calls our adapter to process bytes -> receipt -> webhook.
    """
    return await ingest_csv_handler(
        file=file,
        gmail_id=gmail_id,
        received_date=received_date,
        original_name=original_name,
        google_drive_url=google_drive_url,
        authorization=authorization,
        background_tasks=background_tasks,
        gcs_bucket=settings.gcs_bucket,
        intake_token=(settings.intake_token or ""),  # empty means "no auth"
        process_csv_direct_func=lambda csv_bytes, gcs_path, gcs_bucket, google_drive_url=None: process_csv_from_bytes(
            csv_bytes=csv_bytes,
            gcs_path=gcs_path,
            gcs_bucket=gcs_bucket,
            human_source_url=google_drive_url,
            webhook=webhook_client,
        ),
    )


# -----------------------------------------------------------------------------
# Processing (GCS -> transform -> webhook)
# -----------------------------------------------------------------------------
@app.post("/process-csv")
async def process_csv_file(
    background_tasks: BackgroundTasks,
    gcs_path: str = Body(..., embed=True),
):
    """
    Enqueue background processing for an already-uploaded GCS CSV.
    """
    try:
        background_tasks.add_task(
            process_csv_from_gcs,
            gcs_path=gcs_path,
            gcs_bucket=settings.gcs_bucket,
            webhook=webhook_client,
        )
        return {
            "status": "accepted",
            "message": f"Processing started for {gcs_path}",
            "timestamp": datetime.utcnow().isoformat(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to start processing")
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------
@app.get("/")
@app.head("/")
def root():
    return {"status": "ok", "service": "fintech-etl"}

@app.get("/health")
@app.head("/health")
def health_check():
    """
    Lightweight health probe. Avoids slow GCS I/O; only checks client creation.
    """
    try:
        storage.Client()
        ok = True
    except Exception as e:
        logger.warning("GCS client init failed: %s", e)
        ok = False

    return {
        "status": "healthy" if ok else "degraded",
        "gcs_client": "ok" if ok else "error",
        "bucket": settings.gcs_bucket,
        "webhook_configured": bool(settings.webhook_url),
    }

@app.get("/list-pending")
def list_pending_files():
    """
    List CSVs under raw/ for visibility/backfill.
    """
    try:
        client = get_storage_client()
        bucket = client.bucket(settings.gcs_bucket)
        csv_files = []
        for blob in bucket.list_blobs(prefix="raw/"):
            if blob.name.endswith(".csv"):
                csv_files.append({
                    "name": blob.name,
                    "size": blob.size,
                    "created": blob.time_created.isoformat() if blob.time_created else None,
                    "gcs_path": f"gs://{settings.gcs_bucket}/{blob.name}",
                })
        return {"status": "ok", "count": len(csv_files), "files": csv_files}
    except Exception as e:
        logger.exception("Failed to list pending")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-all-pending")
async def process_all_pending(background_tasks: BackgroundTasks):
    """
    Fan out processing of all pending raw/*.csv files.
    """
    try:
        listing = list_pending_files()
        count = listing.get("count", 0)
        files = listing.get("files", [])

        if count == 0:
            return {"status": "ok", "message": "No pending files to process"}

        for f in files:
            background_tasks.add_task(
                process_csv_from_gcs,
                gcs_path=f["name"],
                gcs_bucket=settings.gcs_bucket,
                webhook=webhook_client,
            )

        return {
            "status": "accepted",
            "message": f"Started processing {count} files",
            "files": [f["name"] for f in files],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Failed to kick off process-all-pending")
        raise HTTPException(status_code=500, detail=str(e))