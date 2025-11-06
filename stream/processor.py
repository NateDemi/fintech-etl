"""
CSV processor that transforms vendor invoice data to receipt schema
"""
import pandas as pd
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from .schema import LineItem, ProcessedReceipt
from rules import QuantityRule, PriceRule, InvoiceRule, ItemRule

logger = logging.getLogger(__name__)


class CSVToReceiptProcessor:
    """Processes vendor invoice CSV data and transforms it to receipt schema"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
        self.item_rule = ItemRule()
        self.quantity_rule = QuantityRule(item_rule=self.item_rule)
        self.price_rule = PriceRule()
        self.invoice_rule = InvoiceRule()
    
    def _generate_document_id(self, gmail_id: str, invoice_number: str = None) -> str:
        """Generate unique document ID: fnt-{gmail_id}-{invoice_number}-{timestamp_seconds}"""
        timestamp = int(datetime.now().timestamp())
        if invoice_number:
            return f"fnt-{gmail_id}-{invoice_number}-{timestamp}"
        else:
            return f"fnt-{gmail_id}-{timestamp}"
    
    def process_vendor_invoice(self, csv_data: pd.DataFrame, gcs_path: str, google_drive_url: str = None, gmail_id: str = None) -> List[ProcessedReceipt]:
        """Transform vendor invoice CSV data to receipt schema - handles multiple invoices"""
        if csv_data.empty:
            return []
        
        # Group by Invoice Number to handle multiple invoices in one CSV
        invoice_groups = csv_data.groupby('Invoice Number')
        receipts = []
        
        for invoice_number, invoice_data in invoice_groups:
            # Get invoice number using InvoiceRule for consistency
            first_row = invoice_data.iloc[0]
            invoice_number_str = self.invoice_rule.get_invoice_number(first_row)
            receipt = self._create_receipt_from_invoice(invoice_data, invoice_number_str, gcs_path, google_drive_url, gmail_id)
            receipts.append(receipt)
        
        return receipts
    
    def _create_receipt_from_invoice(self, invoice_data: pd.DataFrame, invoice_number: str, gcs_path: str, google_drive_url: str = None, gmail_id: str = None) -> ProcessedReceipt:
        """Create a single receipt from invoice data"""
        
        first_row = invoice_data.iloc[0]
        
        line_items = []
        for _, row in invoice_data.iterrows():
            line_item = self._create_line_item_from_row(row)
            line_items.append(line_item)
        
        total_amount = self.invoice_rule.get_invoice_amount(first_row)
        item_count = len(line_items)
        
        subtotal = sum(self.price_rule.get_extended_price(row) for _, row in invoice_data.iterrows())
        sales_tax = self.price_rule.get_tax_amount(first_row)
        source_file = google_drive_url if google_drive_url else f"gs://{self.gcs_bucket}/{gcs_path}"
        unique_document_id = self._generate_document_id(gmail_id, invoice_number)
        
        return ProcessedReceipt(
            receipt_id=invoice_number,
            vendor=self.invoice_rule.get_vendor_name(first_row),
            transaction_date=self.invoice_rule._parse_date(self.invoice_rule.get_invoice_date(first_row)),
            total_amount=total_amount,
            sales_tax=sales_tax,
            subtotal=subtotal,
            item_count=item_count,
            line_items=line_items,
            source_file=source_file,
            processed_at=datetime.now().isoformat(),
            gcs_bucket=self.gcs_bucket,
            gcs_path=gcs_path,
            document_id=unique_document_id
        )
    
    def _create_line_item_from_row(self, row: pd.Series) -> LineItem:
        """Create a line item from a CSV row"""
        product_description = self.item_rule.get_item_name(row)
        product_number = self.item_rule.get_item_number(row)
        
        return LineItem(
            name=product_description,
            qty=self._calculate_quantity(row),
            price=self.price_rule.get_extended_price(row),
            discount=self.price_rule.get_discount_amount(row),
            upc=self.item_rule.extract_upc(row),
            sku=self.item_rule.format_sku(row.get('Case UPC', '')),
            text=product_description,
            unitOfMeasure=self.item_rule._extract_unit_of_measure(row.get('Unit Of Measure', '')),
            category=self.quantity_rule._identify_product_category(row),
            tax=self.price_rule.get_tax_amount(row),
            notes=self._extract_notes(row),
            packs_per_case=self.quantity_rule.get_packs_per_case(row),
            units_per_pack=self.quantity_rule.get_units_per_pack(row)
        )
    
    
    def _extract_email_id(self, gcs_path: str) -> str:
        """Extract email ID from GCS path"""
        try:
            filename = gcs_path.split('/')[-1]
            parts = filename.split('_')
            return parts[1] if len(parts) >= 2 else "unknown"
        except:
            return "unknown"
    
    def _calculate_quantity(self, row: pd.Series) -> int:
        """Calculate total quantity using QuantityRule."""
        return self.quantity_rule.calculate_quantity(row)
    
    def _extract_notes(self, row: pd.Series) -> Optional[str]:
        """Extract notes from adjustment fields"""
        notes = []
        discount = self.price_rule.get_discount_amount(row)
        deposit = self.price_rule.get_deposit_amount(row)
        misc = self.price_rule.get_miscellaneous_amount(row)
        delivery = self.price_rule.get_delivery_amount(row)
        
        if discount != 0:
            notes.append(f"Discount: {discount}")
        if deposit != 0:
            notes.append(f"Deposit: {deposit}")
        if misc != 0:
            notes.append(f"Misc: {misc}")
        if delivery != 0:
            notes.append(f"Delivery: {delivery}")
            
        return '; '.join(notes) if notes else None
