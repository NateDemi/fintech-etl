"""
Invoice Metadata Rules

This module contains business logic for handling invoice-level metadata from CSV data:
- Vendor Name, Retailer Name, Store Numbers
- Invoice dates, numbers, amounts
- Process metadata
"""

import pandas as pd
from .base import BaseRule


class InvoiceRule(BaseRule):
    """Handles invoice-level metadata extraction from CSV data."""
    
    def get_vendor_name(self, row: pd.Series) -> str:
        """Get Vendor Name from CSV data."""
        return str(row.get('Vendor Name', 'Unknown Vendor')).strip()
    
    def get_retailer_name(self, row: pd.Series) -> str:
        """Get Retailer Name from CSV data."""
        return str(row.get('Retailer Name', '')).strip()
    
    def get_retailer_vendor_id(self, row: pd.Series) -> str:
        """Get Retailer VendorID from CSV data."""
        return str(row.get('Retailer VendorID', '')).strip()
    
    def get_vendor_store_number(self, row: pd.Series) -> str:
        """Get Vendor Store Number from CSV data."""
        return str(row.get('Vendor Store Number', '')).strip()
    
    def get_retailer_store_number(self, row: pd.Series) -> str:
        """Get Retailer Store Number from CSV data."""
        return str(row.get('Retailer Store Number', '')).strip()
    
    def get_fintech_process_date(self, row: pd.Series) -> str:
        """Get Fintech Process Date from CSV data."""
        return str(row.get('Fintech Process Date', '')).strip()
    
    def get_invoice_date(self, row: pd.Series) -> str:
        """Get Invoice Date from CSV data."""
        return str(row.get('Invoice Date', '')).strip()
    
    def get_invoice_due_date(self, row: pd.Series) -> str:
        """Get Invoice DueDate from CSV data."""
        return str(row.get('Invoice DueDate', '')).strip()
    
    def get_invoice_number(self, row: pd.Series) -> str:
        """Get Invoice Number from CSV data."""
        return str(row.get('Invoice Number', '')).strip()
    
    def get_invoice_amount(self, row: pd.Series) -> float:
        """Get Invoice Amount from CSV data."""
        return self._num(row, "Invoice Amount", 0.0)
    
    def get_invoice_item_count(self, row: pd.Series) -> int:
        """Get Invoice Item Count from CSV data."""
        return int(self._num(row, "Invoice Item Count", 0))
