"""
CSV processor that transforms vendor invoice data to receipt schema
"""
import pandas as pd
import logging
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from .schema import LineItem, ProcessedReceipt

logger = logging.getLogger(__name__)

class CSVToReceiptProcessor:
    """Processes vendor invoice CSV data and transforms it to receipt schema"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
    
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
            receipt = self._create_receipt_from_invoice(invoice_data, invoice_number, gcs_path, google_drive_url, gmail_id)
            receipts.append(receipt)
        
        return receipts
    
    def _create_receipt_from_invoice(self, invoice_data: pd.DataFrame, invoice_number: str, gcs_path: str, google_drive_url: str = None, gmail_id: str = None) -> ProcessedReceipt:
        """Create a single receipt from invoice data"""
        
        first_row = invoice_data.iloc[0]
        
        line_items = []
        for _, row in invoice_data.iterrows():
            line_item = self._create_line_item_from_row(row)
            line_items.append(line_item)
        
        total_amount = float(first_row.get('Invoice Amount', 0))
        item_count = len(line_items)
        
        subtotal = sum(float(row.get('Extended Price', 0)) for _, row in invoice_data.iterrows())
        sales_tax = float(first_row.get('Tax Adjustment Total', 0))
        source_file = google_drive_url if google_drive_url else f"gs://{self.gcs_bucket}/{gcs_path}"
        unique_document_id = self._generate_document_id(gmail_id, invoice_number)
        
        return ProcessedReceipt(
            receipt_id=invoice_number,
            vendor=first_row.get('Vendor Name', 'Unknown Vendor'),
            transaction_date=self._parse_date(first_row.get('Invoice Date', '')),
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
        product_description = str(row.get('Product Description', ''))
        product_number = str(row.get('Product Number', ''))
        
        return LineItem(
            name=product_description,
            qty=self._calculate_quantity(row),
            price=float(row.get('Extended Price', 0)),
            discount=float(row.get('Discount Adjustment Total', 0)),
            upc=self._extract_upc(row),
            sku=self._format_sku(row.get('Case UPC', '')),
            text=product_description,
            unitOfMeasure=self._extract_unit_of_measure(row.get('Unit Of Measure', '')),
            category=row.get('Product Class', 'Other'),
            tax=0,
            notes=self._extract_notes(row)
        )
    
    def _parse_date(self, date_str: str) -> date:
        """Parse date string to date object"""
        if not date_str or date_str == 'nan':
            return date.today()
        
        try:
            # Try MM/DD/YYYY format
            return datetime.strptime(date_str, '%m/%d/%Y').date()
        except ValueError:
            try:
                # Try YYYY-MM-DD format
                return datetime.strptime(date_str, '%Y-%m-%d').date()
            except ValueError:
                return date.today()
    
    def _extract_email_id(self, gcs_path: str) -> str:
        """Extract email ID from GCS path"""
        try:
            filename = gcs_path.split('/')[-1]
            parts = filename.split('_')
            return parts[1] if len(parts) >= 2 else "unknown"
        except:
            return "unknown"
    
    def _calculate_quantity(self, row: pd.Series) -> int:
        """Calculate quantity: Quantity × Packs Per Case (if not zero), otherwise Quantity × 1"""
        quantity = float(row.get('Quantity', 0)) or 1
        packs_per_case = float(row.get('Packs Per Case', 0)) or 0
        
        if packs_per_case != 0:
            quantity = quantity * packs_per_case
        else:
            quantity = quantity * 1  
        
        return int(quantity)
    
    def _format_sku(self, case_upc: str) -> Optional[str]:
        """Format SKU with leading zeros (14 digits)"""
        if not case_upc or str(case_upc) == 'nan' or not str(case_upc).strip():
            return None
        
        upc = str(case_upc).strip()
        upc = upc.zfill(14)
        return upc[:14]
    
    def _extract_upc(self, row: pd.Series) -> Optional[str]:
        """Extract UPC with priority: Pack UPC → Clean UPC → Case UPC"""
        upc_fields = ['Pack UPC', 'Clean UPC', 'Case UPC']
        
        for field in upc_fields:
            upc = str(row.get(field, ''))
            if upc and upc != 'nan' and upc.strip():
                upc = upc.strip()
                upc = upc.zfill(14)
                return upc[:14]  # Ensure 14 digits max
        
        return None
    
    def _extract_unit_of_measure(self, uom: str) -> str:
        """Extract unit of measure"""
        if not uom or str(uom) == 'nan':
            return 'unit'
        
        uom_lower = str(uom).lower()
        if 'oz' in uom_lower:
            return 'oz'
        elif 'ct' in uom_lower or 'count' in uom_lower:
            return 'ct'
        elif 'pack' in uom_lower:
            return 'pack'
        else:
            return 'unit'
    
    def _calculate_discount(self, row: pd.Series) -> float:
        """Calculate total discount from adjustment fields"""
        return float(row.get('Discount Adjustment Total', 0))
    
    def _extract_notes(self, row: pd.Series) -> Optional[str]:
        """Extract notes from adjustment fields"""
        notes = []
        if float(row.get('Discount Adjustment Total', 0)) != 0:
            notes.append(f"Discount: {row.get('Discount Adjustment Total', 0)}")
        if float(row.get('DepositAdjustmentTotal', 0)) != 0:
            notes.append(f"Deposit: {row.get('DepositAdjustmentTotal', 0)}")
        return '; '.join(notes) if notes else None
