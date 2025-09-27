"""
Gmail to GCS intake handlers
"""
import hashlib
import logging
from datetime import datetime
from typing import Optional

from fastapi import File, Form, UploadFile, HTTPException, Header, BackgroundTasks
from google.cloud import storage

logger = logging.getLogger(__name__)

# Constants
FOLDER_PREFIX = "intake"

def get_storage_client():
    """Get storage client, initializing only when needed."""
    try:
        return storage.Client()
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        return None

def generate_object_name(original_name: str, gmail_id: str, received_date: str, contents: bytes) -> str:
    """Builds unique object name using received_date + Gmail message ID + original name."""
    safe_name = original_name.replace(" ", "_")
    return f"{FOLDER_PREFIX}/{received_date}_{gmail_id}_{safe_name}"

def verify_token(auth_header: str, intake_token: str):
    """Verify authorization token if configured"""
    if not intake_token:
        return  # no token configured
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = auth_header.split(" ")[1]
    if token != intake_token:
        raise HTTPException(status_code=401, detail="Invalid token")

async def ingest_csv_handler(
    file: UploadFile,
    gmail_id: str,
    received_date: str,
    original_name: str,
    google_drive_url: str = None,
    authorization: str = None,
    background_tasks: BackgroundTasks = None,
    gcs_bucket: str = None,
    intake_token: str = None,
    process_csv_direct_func = None
):
    """Handle CSV intake from Gmail and process directly"""
    logger.info(f"📥 Received CSV intake request: {original_name} (Gmail ID: {gmail_id})")
    verify_token(authorization, intake_token)
    
    contents = await file.read()
    object_name = generate_object_name(original_name, gmail_id, received_date, contents)

    # Upload to GCS for backup (optional) - skip if no credentials
    storage_client = get_storage_client()
    if storage_client:
        try:
            logger.info(f"📤 Uploading to GCS backup: gs://{gcs_bucket}/{object_name}")
            bucket = storage_client.bucket(gcs_bucket)
            blob = bucket.blob(object_name)
            blob.upload_from_string(contents, content_type="text/csv")
            logger.info(f"✅ Successfully uploaded to GCS intake folder: {object_name}")
        except Exception as e:
            logger.warning(f"⚠️ GCS upload failed: {e}")
            logger.info("🔄 Continuing with direct processing...")
    else:
        logger.warning("⚠️ GCS client not available (running locally?)")
        logger.info("🔄 Continuing with direct processing...")

    if background_tasks and process_csv_direct_func:
        logger.info("🚀 Starting background processing task...")
        logger.info(f"📊 CSV size: {len(contents)} bytes")
        logger.info(f"📁 GCS path: gs://{gcs_bucket}/{object_name}")
        logger.info(f"🔗 Google Drive URL: {google_drive_url}")
        
        async def process_wrapper():
            try:
                logger.info("⚙️ Background task started - calling process_csv_direct_func")
                result = await process_csv_direct_func(contents, object_name, gcs_bucket, google_drive_url)
                logger.info(f"✅ Background processing completed. Result: {result}")
            except Exception as e:
                logger.error(f"❌ Background processing failed: {e}", exc_info=True)
        
        background_tasks.add_task(process_wrapper)
        logger.info("📋 Background task queued successfully")
    else:
        logger.warning("⚠️ No background tasks or processing function available")

    return {
        "status": "success",
        "message": "CSV uploaded and processing started",
        "gcs_path": f"gs://{gcs_bucket}/{object_name}",
        "gmail_id": gmail_id,
        "original_name": original_name,
        "google_drive_url": google_drive_url,
        "processing": "direct_stream"
    }
