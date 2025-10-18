"""
UPC Code Extraction Rules

This module contains business logic for extracting and formatting UPC codes:
- Clean UPC
- Pack UPC  
- Case UPC
- UPC validation and formatting
"""

import pandas as pd
from typing import Optional
from .base import BaseRule


class CodeRule(BaseRule):
    """Handles UPC code extraction and formatting logic."""
    
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
