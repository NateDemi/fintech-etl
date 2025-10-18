# Fintech ETL Service

A consolidated service that handles CSV intake from Gmail, processes them into receipt schema, and streams to webhooks using a modular business rules architecture.

## Project Structure

```
fintech-etl/
├── main.py                    # 🚀 Main FastAPI service
├── requirements.txt           # 📦 Dependencies
├── start.sh                   # 🐳 Container startup script
├── Dockerfile                 # 🐳 Container configuration
├── README.md                 # 📖 Documentation
├── stream/                   # 📁 Processing modules
│   ├── schema.py            # Pydantic models
│   ├── processor.py         # CSV to receipt processor
│   └── util.py              # Webhook and processing utilities
├── intake/                   # 📁 Intake handlers
│   └── handlers.py          # Gmail intake and GCS upload
├── rules/                    # 📁 Business logic rules
│   ├── __init__.py          # Rules package exports
│   ├── base.py              # Common utilities and helpers
│   ├── quantity.py          # Quantity calculation logic
│   ├── price.py             # Price field extraction
│   ├── invoice.py           # Invoice metadata extraction
│   └── code.py              # UPC code extraction and formatting
└── venv/                    # 📁 Virtual environment
    └── ...                  # Python dependencies
```

## Business Logic

The service uses a modular rules-based architecture for processing vendor invoice data:

### Quantity Calculation

**Basic Logic:**
- If Unit of Measure = "bottle" → Use quantity as-is
- If Quantity = 0 → Return 0
- Otherwise → Apply category-specific rules

**Category-Specific Rules:**
- **Beer**: `Quantity × Packs Per Case × Units Per Pack` (special handling for 12/24 packs)
- **Wine**: `Quantity × Packs Per Case` only
- **Spirits/Non-Alcoholic/Miscellaneous**: `Quantity × Packs Per Case`

### Data Processing

- **UPC Codes**: Priority extraction (Pack UPC → Clean UPC → Case UPC), formatted to 14 digits
- **Product Categories**: Based on GL Code (BEER, WINE, SPIRITS, NON-ALCOHOLIC, MISCELLANEOUS)
- **Unit Normalization**: CA→case, BO→bottle, EA→each, etc.
- **Multiple Invoices**: Each CSV can contain multiple invoices, processed separately

## Features

### 1. Gmail Intake (`/ingest`)
- Receives CSV files from Gmail via AppleScript
- Uploads to Google Cloud Storage with structured naming
- Automatically triggers processing pipeline

### 2. CSV Processing (`/process-csv`)
- Downloads CSV files from GCS
- Transforms vendor invoice data to receipt schema using business rules
- Handles multiple invoices per CSV file
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
  "source_file": "https://drive.google.com/file/d/1IqkwkaKWsI4HS9o-44Hxx7i7cjuQg5ku/view?usp=drivesdk",
  "receiptId": "100277702",
  "vendor": "Premium Distributors of Washington D.C., LLC",
  "date": "2025-09-19",
  "totalAmount": 1457.12,
  "salesTax": 0.0,
  "subtotal": 1457.12,
  "itemCount": 29,
  "document_id": "fnt-199642bd0cb894dd-100277702-1759446272",
  "lineItems": [
    {
      "name": "DAD STRENGTH IPA C24 12OZ 6P",
      "qty": 576,
      "price": 38.35,
      "discount": 2.50,
      "upc": "01234567890123",
      "sku": "01111111111111",
      "text": "DAD STRENGTH IPA C24 12OZ 6P",
      "unitOfMeasure": "unit",
      "category": "BEER",
      "tax": 8.25,
      "notes": "Discount: 2.5; Tax: 8.25"
    }
  ]
}
```

### Key Fields Explained

- **`source_file`**: Google Drive URL (preferred) or GCS path (fallback)
- **`document_id`**: Unique identifier combining Gmail ID, invoice number, and timestamp
- **`qty`**: Calculated using business rules (category-specific quantity logic)
- **`upc`**: Priority-based extraction (Pack UPC → Clean UPC → Case UPC)
- **`sku`**: Case UPC formatted to 14 digits with leading zeros
- **`category`**: Product category based on GL Code (BEER, WINE, SPIRITS, etc.)
- **`unitOfMeasure`**: Normalized unit (case, bottle, each, unit, etc.)
- **`notes`**: Concatenated adjustment details (discount, deposit, misc, delivery)

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

## Processing Flow

1. **Intake**: Gmail → GCS via `/ingest` endpoint
2. **Grouping**: Group CSV rows by Invoice Number (handles multiple invoices per CSV)
3. **Processing**: Apply business rules for each line item (quantity, pricing, UPC extraction)
4. **Receipt Creation**: Create `ProcessedReceipt` for each invoice group
5. **Webhook Streaming**: Send individual webhooks for each receipt
6. **Storage**: Store processed receipts in GCS

## Flow

1. **Gmail → GCS:** AppleScript sends CSV to `/ingest`
2. **GCS → Processing:** Service processes CSV to receipt schema using business rules
3. **Processing → Webhook:** Sends structured data to your webhook
4. **Processing → Storage:** Stores processed receipts in GCS
5. **Processing → Pub/Sub:** Publishes events for downstream systems 
