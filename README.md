# Fintech ETL Service

A consolidated service that handles CSV intake from Gmail, processes them into receipt schema, and streams to webhooks.

## Project Structure

```
fintech-etl/
├── main.py                    # 🚀 Main consolidated service
├── requirements.txt           # 📦 Dependencies
├── README.md                 # 📖 Documentation
├── stream/                   # 📁 Processing modules
│   ├── schema.py            # Pydantic models
│   └── processor.py         # CSV to receipt processor
└── venv/                    # 📁 Virtual environment
    └── ...                  # Python dependencies
```

## Features

### 1. Gmail Intake (`/ingest`)
- Receives CSV files from Gmail via AppleScript
- Uploads to Google Cloud Storage with structured naming
- Automatically triggers processing pipeline

### 2. CSV Processing (`/process-csv`)
- Downloads CSV files from GCS
- Transforms vendor invoice data to receipt schema
- Stores processed receipts in GCS
- Publishes events to Pub/Sub

### 3. Webhook Streaming
- Converts processed receipts to exact webhook schema
- Sends to configured webhook endpoint
- Supports custom headers and authentication

## Configuration

Set environment variables or create a `.env` file:

```bash
# GCS Configuration
GCS_BUCKET=fintech-inbox
PROCESSED_BUCKET=fintech-processed

# Optional intake token for security
INTAKE_TOKEN=your-secret-token-here

# Pub/Sub Configuration
PUBSUB_PROJECT_ID=perfect-rider-446204-h0
PUBSUB_TOPIC=receipt-processing

# Webhook Configuration
WEBHOOK_URL=https://your-webhook-endpoint.com/api/receipts
WEBHOOK_HEADERS={"Authorization": "Bearer your-webhook-token", "X-Custom-Header": "value"}
```

## API Endpoints

### Intake
- `POST /ingest` - Receive CSV from Gmail (multipart/form-data)
- `GET /health` - Health check with GCS connectivity

### Processing
- `POST /process-csv` - Process specific CSV file
- `POST /process-all-pending` - Process all pending CSV files
- `GET /list-pending` - List CSV files awaiting processing

### Testing
- `POST /test-webhook` - Test webhook with sample data

## Webhook Schema

The service sends data in this exact format to your webhook:

```json
{
  "source_file": "gs://fintech-inbox/raw/20250919_12345_invoice.csv",
  "receiptId": "100277702",
  "vendor": "Premium Distributors of Washington D.C., LLC",
  "date": "2025-09-19",
  "totalAmount": 1457.12,
  "salesTax": 0.0,
  "subtotal": 1457.12,
  "itemCount": 29,
  "lineItems": [
    {
      "name": "DAD STRENGTH IPA C24 12OZ 6P",
      "qty": 1,
      "price": 38.35,
      "discount": 0,
      "upc": "860011854104",
      "sku": "65921",
      "text": "DAD STRENGTH IPA C24 12OZ 6P",
      "unitOfMeasure": "unit",
      "category": "Beverages",
      "tax": 0,
      "notes": null
    }
  ]
}
```

## Usage

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment:**
   ```bash
   # Create .env file with your configuration
   echo "WEBHOOK_URL=https://your-webhook.com/api/receipts" > .env
   ```

3. **Run the service:**
   ```bash
   python main.py
   ```

4. **Test webhook:**
   ```bash
   curl -X POST "http://localhost:8000/test-webhook" \
        -H "Content-Type: application/json" \
        -d '{"webhook_url": "https://your-webhook.com/api/receipts"}'
   ```

## Flow

1. **Gmail → GCS:** AppleScript sends CSV to `/ingest`
2. **GCS → Processing:** Service processes CSV to receipt schema
3. **Processing → Webhook:** Sends structured data to your webhook
4. **Processing → Storage:** Stores processed receipts in GCS
5. **Processing → Pub/Sub:** Publishes events for downstream systems# Trigger build
