"""
CSV processor that transforms vendor invoice data to receipt schema
"""
import pandas as pd
from datetime import datetime, date
from typing import List, Dict, Any, Optional
from .schema import ReceiptData, LineItem, ProcessedReceipt

class CSVToReceiptProcessor:
    """Processes vendor invoice CSV data and transforms it to receipt schema"""
    
    def __init__(self, gcs_bucket: str):
        self.gcs_bucket = gcs_bucket
    
    def process_vendor_invoice(self, csv_data: pd.DataFrame, gcs_path: str) -> ProcessedReceipt:
        """Transform vendor invoice CSV data to receipt schema"""
        # Group by invoice to create receipt data
        invoice_groups = csv_data.groupby('Invoice Number')
        
        receipts = []
        for invoice_number, invoice_data in invoice_groups:
            receipt = self._create_receipt_from_invoice(invoice_data, invoice_number, gcs_path)
            receipts.append(receipt)
        
        return receipts[0] if receipts else None
    
    def _create_receipt_from_invoice(self, invoice_data: pd.DataFrame, invoice_number: str, gcs_path: str) -> ProcessedReceipt:
        """Create a single receipt from invoice data"""
        
        # Get invoice-level data (should be same for all rows)
        first_row = invoice_data.iloc[0]
        
        # Create line items from invoice data
        line_items = []
        for _, row in invoice_data.iterrows():
            line_item = self._create_line_item_from_row(row)
            line_items.append(line_item)
        
        # Calculate totals
        total_amount = float(first_row.get('Invoice Amount', 0))
        item_count = len(line_items)
        subtotal = sum(item.price * item.qty for item in line_items)
        sales_tax = max(0, total_amount - subtotal)  # Assume difference is tax
        
        # Create receipt data with detailed source information
        gmail_id = gcs_path.split('_')[1] if '_' in gcs_path else "unknown"
        google_drive_folder_id = "1-79sAJHmIIvYU4NDCUK99GbBoLZhdr-I"  # Your specific Google Drive folder
        
        source_info = {
            "gcs_path": f"gs://{self.gcs_bucket}/{gcs_path}",
            "gmail_id": gmail_id,
            "received_date": gcs_path.split('_')[0] if '_' in gcs_path else "unknown",
            "original_filename": gcs_path.split('_', 2)[-1] if '_' in gcs_path else gcs_path,
            "source_type": "gmail_attachment",
            "google_drive_folder_id": google_drive_folder_id,
            "google_drive_file_url": f"https://drive.google.com/file/d/{gmail_id}/view",
            "google_drive_folder_url": f"https://drive.google.com/drive/folders/{google_drive_folder_id}",
            "google_drive_url": f"https://drive.google.com/file/d/{gmail_id}/view?folderId={google_drive_folder_id}"
        }
        
        receipt_data = ReceiptData(
            source_file=source_info["google_drive_url"],
            receiptId=str(invoice_number),
            vendor=first_row.get('Vendor Name', 'Unknown Vendor'),
            date=self._parse_date(first_row.get('Invoice Date', '')).isoformat(),
            totalAmount=total_amount,
            salesTax=sales_tax,
            subtotal=subtotal,
            itemCount=item_count,
            lineItems=line_items
        )
        
        # Convert to processed receipt
        return ProcessedReceipt(
            receipt_id=receipt_data.receiptId,
            vendor=receipt_data.vendor,
            transaction_date=receipt_data.date,
            total_amount=receipt_data.totalAmount,
            sales_tax=receipt_data.salesTax,
            subtotal=receipt_data.subtotal,
            item_count=receipt_data.itemCount,
            line_items=receipt_data.lineItems,
            source_file=receipt_data.source_file,
            processed_at=datetime.now().isoformat(),
            gcs_bucket=self.gcs_bucket,
            gcs_path=gcs_path
        )
    
    def _create_line_item_from_row(self, row: pd.Series) -> LineItem:
        """Create a line item from a CSV row"""
        
        # Extract product information
        product_description = str(row.get('Product Description', ''))
        product_number = str(row.get('Product Number', ''))
        
        # Parse quantity and price
        quantity = self._parse_quantity(row.get('Quantity', 0))
        price = float(row.get('Invoice Line Item Cost', 0))
        
        # Extract UPC
        upc = self._extract_upc(row)
        
        # Determine category from Product Class field
        category = self._determine_category(row.get('Product Class', ''), product_description)
        
        # Extract unit of measure
        unit_of_measure = self._extract_unit_of_measure(row.get('Unit Of Measure', ''))
        
        # Calculate discount (if any adjustments exist)
        discount = self._calculate_discount(row)
        
        return LineItem(
            name=product_description,
            qty=quantity,
            price=price,
            discount=discount,
            upc=upc,
            sku=product_number if product_number != 'nan' else None,
            text=product_description,
            unitOfMeasure=unit_of_measure,
            category=category,
            tax=0,  # Tax is calculated at receipt level
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
    
    def _parse_quantity(self, qty_str) -> int:
        """Parse quantity string to integer"""
        try:
            if qty_str and str(qty_str) != 'nan':
                qty = int(float(qty_str))
                return qty if qty > 0 else 1  # Ensure at least 1 for receipt schema
            return 1
        except (ValueError, TypeError):
            return 1
    
    def _extract_upc(self, row: pd.Series) -> Optional[str]:
        """Extract UPC from various UPC fields"""
        upc_fields = ['Case UPC', 'Clean UPC', 'Pack UPC']
        
        for field in upc_fields:
            upc = str(row.get(field, ''))
            if upc and upc != 'nan' and upc.strip():
                # Clean up UPC (remove leading zeros, ensure 12 digits)
                upc = upc.strip().lstrip('0')
                if len(upc) < 12:
                    upc = upc.zfill(12)
                return upc[:12]  # Ensure 12 digits max
        
        return None
    
    def _determine_category(self, product_class: str, product_description: str) -> str:
        """Determine product category from Product Class field and description"""
        # First try Product Class field
        if product_class and str(product_class) != 'nan':
            class_lower = str(product_class).lower()
            if 'beer' in class_lower:
                return 'Beverages'
            elif 'spirit' in class_lower:
                return 'Beverages'
            elif 'non-alcoholic' in class_lower:
                return 'Beverages'
            elif 'nonalcohol' in class_lower:
                return 'Beverages'
        
        # Fallback to description analysis
        desc_lower = product_description.lower()
        
        if any(word in desc_lower for word in ['beer', 'ale', 'lager', 'ipa', 'stout']):
            return 'Beverages'
        elif any(word in desc_lower for word in ['wine', 'spirit', 'liquor']):
            return 'Beverages'
        elif any(word in desc_lower for word in ['snack', 'chip', 'cracker']):
            return 'Snacks'
        elif any(word in desc_lower for word in ['candy', 'chocolate', 'sweet']):
            return 'Candy'
        else:
            return 'Other'
    
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
        discount_fields = [
            'Discount Adjustment Total',
            'DepositAdjustmentTotal',
            'Miscellaneous Adjustment Total'
        ]
        
        total_discount = 0
        for field in discount_fields:
            try:
                value = float(row.get(field, 0))
                if value < 0: 
                    total_discount += abs(value)
            except (ValueError, TypeError):
                continue
        
        return total_discount
    
    def _extract_notes(self, row: pd.Series) -> Optional[str]:
        """Extract notes from adjustment fields"""
        notes = []
        
        if float(row.get('Discount Adjustment Total', 0)) != 0:
            notes.append(f"Discount: {row.get('Discount Adjustment Total', 0)}")
        
        if float(row.get('DepositAdjustmentTotal', 0)) != 0:
            notes.append(f"Deposit: {row.get('DepositAdjustmentTotal', 0)}")
        
        return '; '.join(notes) if notes else None
