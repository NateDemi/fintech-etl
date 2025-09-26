"""
Processing logic for CSV intake and webhook streaming (simplified)
"""
from __future__ import annotations

import json
import logging
from io import StringIO
from typing import Optional, Dict

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
        if not self.is_configured():
            logger.info("Webhook not configured; skipping send.")
            return

        payload = to_webhook_schema(receipt)
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(self.url, json=payload, headers=self.headers) as resp:
                    text = await resp.text()
                    if 200 <= resp.status < 300:
                        logger.info("Webhook ok for receipt %s (status=%s)", receipt.receipt_id, resp.status)
                    else:
                        logger.error("Webhook error status=%s body=%s", resp.status, text)
        except Exception as e:
            logger.exception("Webhook send failed: %s", e)


def to_webhook_schema(r: ProcessedReceipt) -> dict:
    """Map internal model -> webhook schema."""
    return {
        "receiptId": r.receipt_id,
        "vendor": r.vendor,
        "transactionDate": r.transaction_date,  # if this is a date/datetime, your Pydantic model should json-encode it
        "totalAmount": r.total_amount,
        "salesTax": r.sales_tax,
        "subtotal": r.subtotal,
        "itemCount": r.item_count,
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
) -> Optional[ProcessedReceipt]:
    """
    Process a CSV already in memory and optionally send it to a webhook.
    Returns the ProcessedReceipt (or None if no data).
    """
    try:
        df = _read_csv_from_bytes(csv_bytes)
        logger.info("CSV loaded rows=%d cols=%d (path=%s)", len(df), len(df.columns), gcs_path)

        processor = CSVToReceiptProcessor(gcs_bucket)
        receipt = processor.process_vendor_invoice(df, gcs_path, human_source_url)
        if not receipt:
            logger.warning("No receipt produced for %s", gcs_path)
            return None

        _ensure_source_fields(receipt, gcs_bucket, gcs_path, human_source_url)

        if webhook.is_configured():
            await webhook.send(receipt)
        return receipt

    except Exception as e:
        logger.exception("Failed processing %s: %s", gcs_path, e)
        return None


async def process_csv_from_gcs(
    *,
    gcs_path: str,
    gcs_bucket: str,
    webhook: WebhookClient,
    storage_client: storage.Client | None = None,
) -> Optional[ProcessedReceipt]:
    """
    Download CSV from GCS, process, and optionally send to webhook.
    """
    try:
        storage_client = storage_client or storage.Client()
        blob = storage_client.bucket(gcs_bucket).blob(gcs_path)
        csv_text = blob.download_as_text()  # server-side charset detection is minimal; fallback is ok
        df = pd.read_csv(StringIO(csv_text))
        logger.info("GCS CSV loaded rows=%d cols=%d (path=%s)", len(df), len(df.columns), gcs_path)

        processor = CSVToReceiptProcessor(gcs_bucket)
        receipt = processor.process_vendor_invoice(df, gcs_path, None)
        if not receipt:
            logger.warning("No receipt produced for %s", gcs_path)
            return None

        _ensure_source_fields(receipt, gcs_bucket, gcs_path, None)

        if webhook.is_configured():
            await webhook.send(receipt)
        return receipt

    except Exception as e:
        logger.exception("Failed GCS processing %s: %s", gcs_path, e)
        return None