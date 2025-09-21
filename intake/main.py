import os
import hashlib
import csv
import io
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

@app.get("/healthz")
def health_check():
    return {"status": "alive"}

@app.post("/test-gcs")
async def test_gcs_connection(authorization: str = Header(None)):
    """Test GCS connection by creating, uploading, and deleting a fake CSV file."""
    verify_token(authorization)
    
    try:
        # Create a fake CSV file in memory
        fake_csv_data = io.StringIO()
        writer = csv.writer(fake_csv_data)
        writer.writerow(["test_id", "test_name", "test_value", "timestamp"])
        writer.writerow(["1", "Test Row 1", "100.50", datetime.utcnow().isoformat()])
        writer.writerow(["2", "Test Row 2", "200.75", datetime.utcnow().isoformat()])
        writer.writerow(["3", "Test Row 3", "300.25", datetime.utcnow().isoformat()])
        
        csv_content = fake_csv_data.getvalue().encode('utf-8')
        
        # Generate object name for test file
        test_gmail_id = "test-gcs-connection"
        test_filename = "test-connection.csv"
        object_name = generate_object_name(test_filename, test_gmail_id, csv_content)
        
        # Upload to GCS
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(object_name)
        blob.upload_from_string(csv_content, content_type="text/csv")
        
        # Log the upload
        upload_log = {
            "action": "upload",
            "bucket": BUCKET_NAME,
            "object": object_name,
            "size": len(csv_content),
            "timestamp": datetime.utcnow().isoformat()
        }
        print(f"GCS Test Upload: {upload_log}")
        
        # Delete the test file
        blob.delete()
        
        # Log the deletion
        delete_log = {
            "action": "delete",
            "bucket": BUCKET_NAME,
            "object": object_name,
            "timestamp": datetime.utcnow().isoformat()
        }
        print(f"GCS Test Delete: {delete_log}")
        
        return {
            "status": "success",
            "message": "GCS connection test completed successfully",
            "bucket": BUCKET_NAME,
            "test_object": object_name,
            "upload_log": upload_log,
            "delete_log": delete_log
        }
        
    except Exception as e:
        error_log = {
            "action": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
        print(f"GCS Test Error: {error_log}")
        raise HTTPException(status_code=500, detail=f"GCS test failed: {str(e)}")

@app.get("/test-gcs-read")
async def test_gcs_read_permissions(authorization: str = Header(None)):
    """Test GCS read permissions by listing bucket contents."""
    verify_token(authorization)
    
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(BUCKET_NAME)
        
        # Test 1: List objects in bucket
        blobs = list(bucket.list_blobs(max_results=10))
        blob_list = []
        for blob in blobs:
            blob_list.append({
                "name": blob.name,
                "size": blob.size,
                "created": blob.time_created.isoformat() if blob.time_created else None,
                "updated": blob.updated.isoformat() if blob.updated else None
            })
        
        # Test 2: Get bucket metadata
        bucket_metadata = {
            "name": bucket.name,
            "location": bucket.location,
            "storage_class": bucket.storage_class,
            "time_created": bucket.time_created.isoformat() if bucket.time_created else None
        }
        
        read_log = {
            "action": "read_test",
            "bucket": BUCKET_NAME,
            "objects_found": len(blob_list),
            "timestamp": datetime.utcnow().isoformat()
        }
        print(f"GCS Read Test: {read_log}")
        
        return {
            "status": "success",
            "message": "GCS read permissions test completed successfully",
            "bucket_metadata": bucket_metadata,
            "recent_objects": blob_list,
            "read_log": read_log
        }
        
    except Exception as e:
        error_log = {
            "action": "read_error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
        print(f"GCS Read Test Error: {error_log}")
        raise HTTPException(status_code=500, detail=f"GCS read test failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)