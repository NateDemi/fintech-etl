"""
Processing logic for CSV intake and webhook streaming (simplified)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from io import StringIO
from typing import Optional, Dict, List

import aiohttp
import pandas as pd
from google.cloud import storage

from .processor import CSVToReceiptProcessor
from .schema import ProcessedReceipt

logger = logging.getLogger(__name__)


class WebhookClient:
    """Lightweight client for posting processed receipts to a webhook."""
    def __init__(self, url: str | None, headers: Dict[str, str] | None = None, timeout_sec: int = 30):
        self.url = (url or "").strip()
        self.headers = {"Content-Type": "application/json", "User-Agent": "fintech-etl-service/1.0"}
        if headers:
            self.headers.update(headers)
        self.timeout = aiohttp.ClientTimeout(total=timeout_sec)

    def is_configured(self) -> bool:
        return bool(self.url)

    async def send(self, receipt: ProcessedReceipt) -> None:
        logger.info(f"🔗 Webhook send attempt for receipt {receipt.receipt_id}")
        
        if not self.is_configured():
            logger.warning("⚠️ Webhook not configured; skipping send.")
            return

        payload = to_webhook_schema(receipt)
        logger.info(f"📦 Webhook payload prepared: {len(str(payload))} chars")
        logger.info(f"🎯 Sending to URL: {self.url}")
        logger.info(f"📋 Payload preview: {str(payload)[:200]}...")
        
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                logger.info("🌐 Making HTTP POST request to webhook...")
                async with session.post(self.url, json=payload, headers=self.headers) as resp:
                    text = await resp.text()
                    logger.info(f"📡 Webhook response: status={resp.status}, body_length={len(text)}")
                    
                    if 200 <= resp.status < 300:
                        logger.info("✅ Webhook SUCCESS for receipt %s (status=%s)", receipt.receipt_id, resp.status)
                        logger.info(f"📄 Response body: {text[:200]}...")
                    else:
                        logger.error("❌ Webhook ERROR status=%s body=%s", resp.status, text)
        except Exception as e:
            logger.error(f"💥 Webhook send FAILED: {e}", exc_info=True)



def to_webhook_schema(r: ProcessedReceipt) -> dict:
    """Map internal model -> webhook schema."""
    return {
        "receiptId": r.receipt_id,
        "vendor": r.vendor,
        "transactionDate": r.transaction_date.isoformat() if r.transaction_date else None,  # Convert date to ISO string
        "totalAmount": r.total_amount,
        "salesTax": r.sales_tax,
        "subtotal": r.subtotal,
        "itemCount": r.item_count,
        "document_id": r.document_id,  
        "lineItems": [
            {
                "name": li.name,
                "qty": li.qty,
                "price": li.price,
                "discount": li.discount,
                "upc": li.upc,
                "sku": li.sku,
                "text": li.text,
                "unitOfMeasure": li.unitOfMeasure,
                "category": li.category,
                "tax": li.tax,
                "notes": li.notes,
                "packs_per_case": li.packs_per_case,
                "units_per_pack": li.units_per_pack,
            }
            for li in r.line_items
        ],
        "source_file": r.source_file,  # human link (Drive or gs://)
    }


def _read_csv_from_bytes(csv_bytes: bytes) -> pd.DataFrame:
    """Best-effort decode + read CSV into DataFrame."""
    text = csv_bytes.decode("utf-8", errors="replace")
    return pd.read_csv(StringIO(text))


def _ensure_source_fields(r: ProcessedReceipt, gcs_bucket: str, gcs_path: str, human_source: Optional[str]) -> None:
    """Guarantee source_file/gcs_path/gcs_bucket are set on the model."""
    if not getattr(r, "gcs_bucket", None):
        r.gcs_bucket = gcs_bucket
    if not getattr(r, "gcs_path", None):
        r.gcs_path = gcs_path
    if not getattr(r, "source_file", None):
        r.source_file = human_source or f"gs://{gcs_bucket}/{gcs_path}"


async def process_csv_from_bytes(
    csv_bytes: bytes,
    *,
    gcs_path: str,
    gcs_bucket: str,
    human_source_url: Optional[str],
    webhook: WebhookClient,
    gmail_id: Optional[str] = None,
) -> List[ProcessedReceipt]:
    """
    Process a CSV already in memory and optionally send it to a webhook.
    Returns a list of ProcessedReceipt objects (one per invoice).
    """
    try:
        logger.info(f"🔄 Starting CSV processing for path: {gcs_path}")
        logger.info(f"📊 CSV bytes: {len(csv_bytes)} bytes")
        logger.info(f"🔗 Human source URL: {human_source_url}")
        logger.info(f"🌐 Webhook configured: {webhook.is_configured()}")
        
        df = _read_csv_from_bytes(csv_bytes)
        logger.info(f"📈 CSV loaded: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"📋 CSV columns: {list(df.columns)}")

        processor = CSVToReceiptProcessor(gcs_bucket)
        logger.info("⚙️ Processing vendor invoices...")
        receipts = processor.process_vendor_invoice(df, gcs_path, human_source_url, gmail_id)
        
        if not receipts:
            logger.warning("⚠️ No receipts produced for %s", gcs_path)
            return []

        logger.info(f"✅ Created {len(receipts)} receipts from CSV")
        
        # Process all receipts
        for i, receipt in enumerate(receipts, 1):
            logger.info(f"📄 Processing receipt {i}/{len(receipts)}: ID={receipt.receipt_id}, Vendor={receipt.vendor}")
            _ensure_source_fields(receipt, gcs_bucket, gcs_path, human_source_url)
            logger.info(f"🔗 Source file set to: {receipt.source_file}")
            logger.info(f"🎉 Processing completed successfully for receipt {receipt.receipt_id}")

        # Send each receipt individually
        for i, receipt in enumerate(receipts, 1):
            if webhook.is_configured():
                logger.info(f"🚀 Sending receipt {i}/{len(receipts)} to webhook...")
                await webhook.send(receipt)
            else:
                logger.warning("⚠️ Webhook not configured - skipping send")
            
        return receipts

    except Exception as e:
        logger.error(f"💥 Failed processing {gcs_path}: {e}", exc_info=True)
        return []


async def process_csv_from_gcs(
    *,
    gcs_path: str,
    gcs_bucket: str,
    webhook: WebhookClient,
    storage_client: storage.Client | None = None,
    gmail_id: Optional[str] = None,
) -> List[ProcessedReceipt]:
    """
    Download CSV from GCS, process, and optionally send to webhook.
    Returns a list of ProcessedReceipt objects (one per invoice).
    """
    try:
        storage_client = storage_client or storage.Client()
        blob = storage_client.bucket(gcs_bucket).blob(gcs_path)
        csv_text = blob.download_as_text()  # server-side charset detection is minimal; fallback is ok
        df = pd.read_csv(StringIO(csv_text))
        logger.info("GCS CSV loaded rows=%d cols=%d (path=%s)", len(df), len(df.columns), gcs_path)

        processor = CSVToReceiptProcessor(gcs_bucket)
        receipts = processor.process_vendor_invoice(df, gcs_path, None, gmail_id)
        if not receipts:
            logger.warning("No receipts produced for %s", gcs_path)
            return []

        logger.info(f"✅ Created {len(receipts)} receipts from CSV")

        # Process all receipts
        for i, receipt in enumerate(receipts, 1):
            logger.info(f"📄 Processing receipt {i}/{len(receipts)}: ID={receipt.receipt_id}, Vendor={receipt.vendor}")
            _ensure_source_fields(receipt, gcs_bucket, gcs_path, None)
            logger.info(f"🔗 Source file set to: {receipt.source_file}")

        # Send each receipt individually
        for i, receipt in enumerate(receipts, 1):
            if webhook.is_configured():
                logger.info(f"🚀 Sending receipt {i}/{len(receipts)} to webhook...")
                await webhook.send(receipt)
            else:
                logger.warning("⚠️ Webhook not configured - skipping send")

        return receipts

    except Exception as e:
        logger.exception("Failed GCS processing %s: %s", gcs_path, e)
        return []