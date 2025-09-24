"""
Consolidated Fintech ETL Service
Handles CSV intake from Gmail, GCS storage, and webhook streaming
"""
import os
import hashlib
import asyncio
import json
import logging
from datetime import datetime, date
from typing import List, Optional

import pandas as pd
import aiohttp
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Header, BackgroundTasks
from google.cloud import storage
from google.cloud import pubsub_v1
from pydantic_settings import BaseSettings

from stream.schema import ProcessedReceipt
from stream.processor import CSVToReceiptProcessor
from intake.handlers import ingest_csv_handler

# Configure logging for Cloud Run
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add startup logging
logger.info("🚀 Starting Fintech ETL Service")

class Settings(BaseSettings):
    # GCS Configuration
    gcs_bucket: str = "fintech-inbox"
    processed_bucket: str = "fintech-processed"
    intake_token: str = ""  # Optional token for intake endpoint
    
    # Pub/Sub Configuration
    pubsub_project_id: str = "perfect-rider-446204-h0"
    pubsub_topic: str = "receipt-processing"
    
    # Webhook Configuration
    webhook_url: str = ""  # Webhook URL for sending processed receipts
    webhook_headers: dict = {}  # Custom headers for webhook
    
    class Config:
        env_file = ".env"
        extra = "ignore"  # Ignore extra environment variables

settings = Settings()
app = FastAPI(title="Fintisech ETL Service", version="1.0.0")

# Log configuration after settings are loaded
logger.info(f"📦 GCS Bucket: {settings.gcs_bucket}")
logger.info(f"🔗 Webhook URL: {settings.webhook_url[:50] + '...' if settings.webhook_url else 'Not configured'}")

def get_storage_client():
    """Get storage client, initializing only when needed."""
    try:
        return storage.Client()
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        raise HTTPException(status_code=500, detail=f"Storage client initialization failed: {str(e)}")

# ============================================================================
# INTAKE ENDPOINTS (Gmail to GCS)
# ============================================================================

@app.post("/ingest")
async def ingest_csv(
    file: UploadFile = File(...),
    gmail_id: str = Form(...),
    received_date: str = Form(...),
    original_name: str = Form(...),
    authorization: str = Header(None),
    background_tasks: BackgroundTasks = None
):
    """Receive CSV attachment and process directly (stream on the fly)"""
    return await ingest_csv_handler(
        file=file,
        gmail_id=gmail_id,
        received_date=received_date,
        original_name=original_name,
        authorization=authorization,
        background_tasks=background_tasks,
        gcs_bucket=settings.gcs_bucket,
        intake_token=settings.intake_token,
        process_csv_direct_func=process_csv_direct
    )

# ============================================================================
# PROCESSING ENDPOINTS (CSV to Webhook)
# ============================================================================

@app.post("/process-csv")
async def process_csv_file(
    gcs_path: str,
    background_tasks: BackgroundTasks
):
    """Process a CSV file from GCS and transform it to receipt schema"""
    try:
        background_tasks.add_task(process_csv_async, gcs_path)
        return {
            "status": "accepted",
            "message": f"Processing started for {gcs_path}",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to start processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def process_csv_direct(csv_contents: bytes, gcs_path: str, gcs_bucket: str):
    """Direct processing of CSV from memory (no GCS download needed)"""
    try:
        logger.info(f"🚀 Starting direct processing of {gcs_path}")
        
        # Save CSV locally for debugging
        local_filename = f"local_csvs/{gcs_path.split('/')[-1]}"
        os.makedirs("local_csvs", exist_ok=True)
        with open(local_filename, 'wb') as f:
            f.write(csv_contents)
        logger.info(f"💾 Saved CSV locally: {local_filename}")
        
        # Process CSV directly from memory
        csv_content = csv_contents.decode('utf-8')
        from io import StringIO
        df = pd.read_csv(StringIO(csv_content))
        
        logger.info(f"📊 Loaded CSV with {len(df)} rows")
        logger.info(f"📋 CSV columns: {list(df.columns)}")
        logger.info(f"📄 First few rows:\n{df.head()}")
        
        # Stream CSV data to console with source information
        print("\n" + "="*80)
        print(f"🔄 STREAMING CSV DATA: {gcs_path}")
        print("="*80)
        print(f"📁 Local File: {local_filename}")
        print(f"📊 Rows: {len(df)}")
        print(f"📋 Columns: {len(df.columns)}")
        
        # Extract source information from gcs_path
        path_parts = gcs_path.split('_')
        if len(path_parts) >= 3:
            received_date = path_parts[0]
            gmail_id = path_parts[1]
            original_filename = '_'.join(path_parts[2:])
            google_drive_folder_id = "1-79sAJHmIIvYU4NDCUK99GbBoLZhdr-I"
            google_drive_file_url = f"https://drive.google.com/file/d/{gmail_id}/view"
            google_drive_folder_url = f"https://drive.google.com/drive/folders/{google_drive_folder_id}"
            google_drive_url = f"https://drive.google.com/file/d/{gmail_id}/view?folderId={google_drive_folder_id}"
            
            print(f"\n📧 SOURCE INFORMATION:")
            print(f"   📅 Received Date: {received_date}")
            print(f"   📧 Gmail ID: {gmail_id}")
            print(f"   📄 Original Filename: {original_filename}")
            print(f"   📁 Google Drive Folder ID: {google_drive_folder_id}")
            print(f"   🔗 Google Drive File URL: {google_drive_file_url}")
            print(f"   📂 Google Drive Folder URL: {google_drive_folder_url}")
            print(f"   🔗 Google Drive URL (with folder): {google_drive_url}")
        
        print("\n📄 CSV Content:")
        print("-" * 80)
        print(df.to_string(index=False))
        print("-" * 80)
        print("✅ CSV data streamed to console")
        print("="*80 + "\n")
        
        # Process CSV to receipt schema
        processor = CSVToReceiptProcessor(gcs_bucket)
        processed_receipt = processor.process_vendor_invoice(df, gcs_path)
        
        if not processed_receipt:
            logger.warning(f"⚠️ No receipt data generated from {gcs_path}")
            return
        
        logger.info(f"✅ Processed receipt: {processed_receipt.receipt_id} from {processed_receipt.vendor}")
        
        # Skip storage for local testing - go straight to webhook
        logger.info("🚀 Skipping storage for local testing, streaming to webhook...")
        
        # Send to webhook if configured
        await send_to_webhook(processed_receipt)
        
        logger.info(f"🎉 Successfully completed direct processing {gcs_path}")
        
    except Exception as e:
        logger.error(f"❌ Failed to process {gcs_path}: {e}")

async def process_csv_async(gcs_path: str):
    """Legacy async processing of CSV file from GCS (for backward compatibility)"""
    try:
        logger.info(f"🔄 Starting background processing of {gcs_path}")
        
        # Download CSV from GCS
        storage_client = get_storage_client()
        bucket = storage_client.bucket(settings.gcs_bucket)
        blob = bucket.blob(gcs_path)
        
        # Read CSV content
        csv_content = blob.download_as_text()
        df = pd.read_csv(pd.StringIO(csv_content))
        
        logger.info(f"📊 Loaded CSV with {len(df)} rows")
        
        # Process CSV to receipt schema
        processor = CSVToReceiptProcessor(settings.gcs_bucket)
        processed_receipt = processor.process_vendor_invoice(df, gcs_path)
        
        if not processed_receipt:
            logger.warning(f"⚠️ No receipt data generated from {gcs_path}")
            return
        
        logger.info(f"✅ Processed receipt: {processed_receipt.receipt_id} from {processed_receipt.vendor}")
        
        # Store processed receipt
        await store_processed_receipt(processed_receipt)
        
        # Publish to Pub/Sub for downstream processing
        await publish_receipt_event(processed_receipt)
        
        # Send to webhook if configured
        await send_to_webhook(processed_receipt)
        
        logger.info(f"🎉 Successfully completed processing {gcs_path}")
        
    except Exception as e:
        logger.error(f"❌ Failed to process {gcs_path}: {e}")

# ============================================================================
# WEBHOOK FUNCTIONALITY
# ============================================================================

def convert_to_webhook_schema(processed_receipt: ProcessedReceipt) -> dict:
    """Convert processed receipt to exact webhook schema format"""
    
    # Convert line items to webhook format
    line_items = []
    for item in processed_receipt.line_items:
        line_item = {
            "name": item.name,
            "qty": item.qty,
            "price": item.price,
            "discount": item.discount,
            "upc": item.upc,
            "sku": item.sku,
            "text": item.text,
            "unitOfMeasure": item.unitOfMeasure,
            "category": item.category,
            "tax": item.tax,
            "notes": item.notes
        }
        line_items.append(line_item)
    
    # Convert to exact webhook schema
    webhook_payload = {
        "source_file": processed_receipt.source_file,
        "receiptId": processed_receipt.receipt_id,
        "vendor": processed_receipt.vendor,
        "date": processed_receipt.transaction_date.isoformat(),
        "totalAmount": processed_receipt.total_amount,
        "salesTax": processed_receipt.sales_tax,
        "subtotal": processed_receipt.subtotal,
        "itemCount": processed_receipt.item_count,
        "lineItems": line_items
    }
    
    return webhook_payload

async def send_to_webhook(processed_receipt: ProcessedReceipt):
    """Send processed receipt to webhook endpoint"""
    logger.info(f"🔗 Attempting to send receipt {processed_receipt.receipt_id} to webhook")
    
    if not settings.webhook_url:
        logger.info("❌ No webhook URL configured, skipping webhook send")
        return
    
    logger.info(f"🌐 Webhook URL: {settings.webhook_url}")
    
    try:
        # Convert to webhook schema
        webhook_payload = convert_to_webhook_schema(processed_receipt)
        logger.info(f"📦 Webhook payload prepared: {len(webhook_payload)} fields")
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "fintech-etl-service/1.0"
        }
        headers.update(settings.webhook_headers)
        
        # Send to webhook
        logger.info(f"🚀 Sending to webhook: {settings.webhook_url}")
        async with aiohttp.ClientSession() as session:
            async with session.post(
                settings.webhook_url,
                json=webhook_payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response_text = await response.text()
                if response.status >= 200 and response.status < 300:
                    logger.info(f"✅ Successfully sent receipt {processed_receipt.receipt_id} to webhook")
                    logger.info(f"📡 Webhook response: {response_text}")
                else:
                    logger.error(f"❌ Webhook returned status {response.status}: {response_text}")
                    
    except Exception as e:
        logger.error(f"❌ Failed to send receipt to webhook: {e}")

# ============================================================================
# STORAGE AND PUB/SUB FUNCTIONS
# ============================================================================

async def store_processed_receipt(receipt: ProcessedReceipt):
    """Store processed receipt in GCS"""
    try:
        # Create output path
        output_path = f"processed/{receipt.receipt_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Store in processed bucket
        storage_client = get_storage_client()
        bucket = storage_client.bucket(settings.processed_bucket)
        blob = bucket.blob(output_path)
        
        # Convert to JSON and upload
        receipt_json = receipt.model_dump_json(indent=2)
        blob.upload_from_string(receipt_json, content_type='application/json')
        
        logger.info(f"Stored processed receipt at gs://{settings.processed_bucket}/{output_path}")
        
    except Exception as e:
        logger.error(f"Failed to store processed receipt: {e}")
        raise

async def publish_receipt_event(receipt: ProcessedReceipt):
    """Publish receipt processing event to Pub/Sub"""
    try:
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(settings.pubsub_project_id, settings.pubsub_topic)
        
        # Create event message
        event_data = {
            "event_type": "receipt_processed",
            "receipt_id": receipt.receipt_id,
            "vendor": receipt.vendor,
            "processed_at": receipt.processed_at,
            "gcs_path": f"gs://{settings.processed_bucket}/processed/{receipt.receipt_id}_*.json"
        }
        
        # Publish message
        message_data = json.dumps(event_data).encode('utf-8')
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()
        
        logger.info(f"Published receipt event: {message_id}")
        
    except Exception as e:
        logger.error(f"Failed to publish receipt event: {e}")

# ============================================================================
# UTILITY ENDPOINTS
# ============================================================================

@app.get("/")
@app.head("/")
@app.post("/")
def root():
    logger.info("🏠 Root endpoint accessed")
    return {"status": "ok", "service": "fintech-etl"}

@app.get("/health")
@app.head("/health")
def health_check():
    """Health check with GCS connectivity test"""
    logger.info("🏥 Health check requested")
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(settings.gcs_bucket)
        bucket_exists = bucket.exists()  # Check if bucket is accessible
        logger.info(f"💚 Health check result: healthy (GCS connected: {bucket_exists})")
        return {
            "status": "healthy", 
            "gcs": "connected", 
            "bucket": settings.gcs_bucket,
            "webhook_configured": bool(settings.webhook_url)
        }
    except Exception as e:
        return {"status": "unhealthy", "gcs": "disconnected", "error": str(e)}

@app.get("/list-pending")
def list_pending_files():
    """List CSV files in GCS that need processing"""
    try:
        storage_client = get_storage_client()
        bucket = storage_client.bucket(settings.gcs_bucket)
        
        # List CSV files in the raw folder
        blobs = bucket.list_blobs(prefix="raw/", delimiter="/")
        csv_files = []
        
        for blob in blobs:
            if blob.name.endswith('.csv'):
                csv_files.append({
                    "name": blob.name,
                    "size": blob.size,
                    "created": blob.time_created.isoformat(),
                    "gcs_path": f"gs://{settings.gcs_bucket}/{blob.name}"
                })
        
        return {
            "status": "ok",
            "count": len(csv_files),
            "files": csv_files
        }
        
    except Exception as e:
        logger.error(f"Failed to list pending files: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-all-pending")
async def process_all_pending(background_tasks: BackgroundTasks):
    """Process all pending CSV files"""
    try:
        # Get list of pending files
        pending_files = list_pending_files()
        
        if pending_files["count"] == 0:
            return {"status": "ok", "message": "No pending files to process"}
        
        # Process each file
        for file_info in pending_files["files"]:
            background_tasks.add_task(process_csv_async, file_info["name"])
        
        return {
            "status": "accepted",
            "message": f"Started processing {pending_files['count']} files",
            "files": [f["name"] for f in pending_files["files"]]
        }
        
    except Exception as e:
        logger.error(f"Failed to process all pending: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/test-webhook")
async def test_webhook(webhook_url: str):
    """Test webhook functionality with sample data"""
    try:
        # Create sample receipt data
        sample_receipt = ProcessedReceipt(
            receipt_id="TEST-12345",
            vendor="Test Vendor",
            transaction_date=date.today(),
            total_amount=100.50,
            sales_tax=8.50,
            subtotal=92.00,
            item_count=2,
            line_items=[
                {
                    "name": "Test Product 1",
                    "qty": 1,
                    "price": 50.00,
                    "discount": 0,
                    "upc": "123456789012",
                    "sku": "SKU001",
                    "text": "Test Product 1",
                    "unitOfMeasure": "unit",
                    "category": "Other",
                    "tax": 0,
                    "notes": None
                },
                {
                    "name": "Test Product 2",
                    "qty": 2,
                    "price": 21.00,
                    "discount": 0,
                    "upc": "123456789013",
                    "sku": "SKU002",
                    "text": "Test Product 2",
                    "unitOfMeasure": "unit",
                    "category": "Other",
                    "tax": 0,
                    "notes": None
                }
            ],
            source_file="gs://test-bucket/test.csv",
            processed_at=datetime.now().isoformat(),
            gcs_bucket="test-bucket",
            gcs_path="test.csv"
        )
        
        # Convert to webhook schema
        webhook_payload = convert_to_webhook_schema(sample_receipt)
        
        # Send test webhook
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "fintech-etl-service/1.0"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json=webhook_payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response_text = await response.text()
                
                return {
                    "status": "success" if response.status < 300 else "error",
                    "status_code": response.status,
                    "response": response_text,
                    "payload": webhook_payload
                }
                
    except Exception as e:
        logger.error(f"Webhook test failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
