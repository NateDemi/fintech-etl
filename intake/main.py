import os
import hashlib
from datetime import datetime
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Header
from google.cloud import storage

BUCKET_NAME = os.environ.get("BUCKET", "fintech-inbox")
INTAKE_TOKEN = os.environ.get("INTAKE_TOKEN", "")
FOLDER_PREFIX = "raw" 

app = FastAPI()

def get_storage_client():
    """Get storage client, initializing only when needed."""
    try:
        return storage.Client()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Storage client initialization failed: {str(e)}")

def generate_object_name(original_name: str, gmail_id: str, contents: bytes) -> str:
    """Builds unique object name using date + Gmail message ID + SHA256 hash."""
    today = datetime.utcnow().strftime("%Y/%m/%d")
    file_hash = hashlib.sha256(contents).hexdigest()[:12]
    safe_name = original_name.replace(" ", "_")
    return f"{FOLDER_PREFIX}/{today}/{gmail_id}_{file_hash}_{safe_name}"

def verify_token(auth_header: str):
    if not INTAKE_TOKEN:
        return  # no token configured
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = auth_header.split(" ", 1)[1]
    if token != INTAKE_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid bearer token")

@app.post("/ingest")
async def ingest_csv(
    file: UploadFile = File(...),
    gmail_id: str = Form(...),
    original_name: str = Form(...),
    authorization: str = Header(None),
):
    """Receive CSV attachment and upload to GCS."""
    verify_token(authorization)
    contents = await file.read()
    object_name = generate_object_name(original_name, gmail_id, contents)

    storage_client = get_storage_client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(object_name)
    blob.upload_from_string(contents, content_type="text/csv")

    return {
        "status": "ok",
        "bucket": BUCKET_NAME,
        "object": object_name,
        "size": len(contents),
    }

@app.get("/")
def root():
    return {"status": "ok", "service": "fintech-intake"}

@app.get("/health")
def health_check():
    """Simple health check with GCS connectivity test."""
    try:
        # Test GCS connection
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        # Just check if we can access the bucket (doesn't require listing)
        bucket.exists()
        return {"status": "healthy", "gcs": "connected", "bucket": BUCKET_NAME}
    except Exception as e:
        return {"status": "unhealthy", "gcs": "disconnected", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)