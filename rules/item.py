"""
Item Information Rules

This module contains business logic for extracting and formatting line item information:
- UPC codes (Clean UPC, Pack UPC, Case UPC)
- Item names and descriptions
- SKU formatting and validation
"""

import pandas as pd
from typing import Optional
from .base import BaseRule


class ItemRule(BaseRule):
    """Handles line item information extraction and formatting logic."""
    
    def extract_upc(self, row: pd.Series) -> Optional[str]:
        """Extract UPC with priority: Pack UPC → Clean UPC → Case UPC"""
        upc_fields = ['Pack UPC', 'Clean UPC', 'Case UPC']
        
        for field in upc_fields:
            upc = str(row.get(field, ''))
            if upc and upc != 'nan' and upc.strip() and upc != 'None':
                upc = upc.strip()
                upc = upc.zfill(14)
                return upc[:14]  # Ensure 14 digits max
        
        return None
    
    def format_sku(self, case_upc: str) -> Optional[str]:
        """Format SKU with leading zeros (14 digits)"""
        if not case_upc or str(case_upc) == 'nan' or str(case_upc) == 'None' or not str(case_upc).strip():
            return None
        
        upc = str(case_upc).strip()
        upc = upc.zfill(14)
        return upc[:14]
    
    def get_clean_upc(self, row: pd.Series) -> Optional[str]:
        """Get Clean UPC from CSV data."""
        upc = str(row.get('Clean UPC', ''))
        if upc and upc != 'nan' and upc != 'None' and upc.strip():
            upc = upc.strip()
            upc = upc.zfill(14)
            return upc[:14]
        return None
    
    def get_pack_upc(self, row: pd.Series) -> Optional[str]:
        """Get Pack UPC from CSV data."""
        upc = str(row.get('Pack UPC', ''))
        if upc and upc != 'nan' and upc != 'None' and upc.strip():
            upc = upc.strip()
            upc = upc.zfill(14)
            return upc[:14]
        return None
    
    def get_case_upc(self, row: pd.Series) -> Optional[str]:
        """Get Case UPC from CSV data."""
        upc = str(row.get('Case UPC', ''))
        if upc and upc != 'nan' and upc != 'None' and upc.strip():
            upc = upc.strip()
            upc = upc.zfill(14)
            return upc[:14]
        return None
    
    def validate_upc(self, upc: str) -> bool:
        """Validate UPC format (14 digits)"""
        if not upc:
            return False
        
        upc = str(upc).strip()
        return upc.isdigit() and len(upc) <= 14
    
    def get_item_name(self, row: pd.Series) -> str:
        """Get Product Description from CSV data."""
        return str(row.get('Product Description', '')).strip()
    
    def get_item_number(self, row: pd.Series) -> str:
        """Get Product Number from CSV data."""
        return str(row.get('Product Number', '')).strip()
    
    def format_item_name(self, name: str) -> str:
        """Format item name (trim, clean up)"""
        if not name or str(name) == 'nan' or str(name) == 'None':
            return ''
        
        return str(name).strip()
    
    def get_product_volume(self, row: pd.Series) -> str:
        """Get Product Volume from CSV data."""
        return str(row.get('Product Volume', '')).strip()
    
    def get_product_class(self, row: pd.Series) -> str:
        """Get Product Class from CSV data."""
        return str(row.get('Product Class', '')).strip()
    
    def get_units_per_pack(self, row: pd.Series) -> int:
        """Get Units Per Pack from CSV data."""
        return int(self._num(row, "Units Per Pack", 1) or 1)
    