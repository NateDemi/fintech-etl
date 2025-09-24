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
        raise HTTPException(status_code=500, detail="GCS client initialization failed")

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
    authorization: str,
    background_tasks: BackgroundTasks,
    gcs_bucket: str,
    intake_token: str,
    process_csv_async_func
):
    """Handle CSV intake from Gmail and upload to GCS"""
    logger.info(f"📥 Received CSV intake request: {original_name} (Gmail ID: {gmail_id})")
    verify_token(authorization, intake_token)
    
    contents = await file.read()
    object_name = generate_object_name(original_name, gmail_id, received_date, contents)

    # Upload to GCS
    logger.info(f"📤 Uploading to GCS: gs://{gcs_bucket}/{object_name}")
    storage_client = get_storage_client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_string(contents, content_type="text/csv")
    logger.info(f"✅ Successfully uploaded to GCS intake folder: {object_name}")

    # Trigger processing in background
    if background_tasks and process_csv_async_func:
        background_tasks.add_task(process_csv_async_func, object_name)

    return {
        "status": "success",
        "message": "CSV uploaded successfully",
        "gcs_path": f"gs://{gcs_bucket}/{object_name}",
        "gmail_id": gmail_id,
        "original_name": original_name
    }
