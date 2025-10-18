"""
Base Rule Class

This module provides the foundation for all business rule classes.
Contains common helper methods, constants, and utilities.
"""

import pandas as pd
from typing import Any, Dict, Set
from datetime import datetime, date


class BaseRule:
    """Base class for all business rules with common helper methods and constants."""
    
    # Category constants - shared across all rules
    BEER = "BEER"
    WINE = "WINE"
    SPIRITS = "SPIRITS"
    NON_ALC = "NON-ALCOHOLIC"
    MISC = "MISCELLANEOUS"
    
    
    
    def _num(self, row: pd.Series, key: str, default: float = 0.0) -> float:
        """Read a numeric field safely."""
        try:
            val = float(row.get(key, default))
            return val if pd.notna(val) else default
        except (TypeError, ValueError):
            return default

    def _text(self, row: pd.Series, key: str) -> str:
        """Read a text field safely, uppercase, trimmed."""
        return str(row.get(key, "")).strip().upper()
    
    
    def _identify_product_category(self, row: pd.Series) -> str:
        """
        Categorization (ordered, explicit):
          - GL contains 'BEER' → BEER
          - GL contains 'WINE' → WINE
          - GL contains 'SPIRIT' → SPIRITS
          - GL contains 'NONALCOHOL' → MISC if Product Class is 'MISCELLANEOUS', else NON-ALCOHOLIC
          - Otherwise → MISC
        """
        gl = self._text(row, "GL Code")
        pc = self._text(row, "Product Class")

        if "BEER" in gl:
            return "BEER"
        if "WINE" in gl:
            return "WINE"
        if "SPIRIT" in gl:
            return "SPIRITS"
        if "NONALCOHOL" in gl:
            return "MISCELLANEOUS" if "MISCELLANEOUS" in pc else "NON-ALCOHOLIC"
        return "MISCELLANEOUS"
    
    def _parse_date(self, date_str: str) -> date:
        """Parse date string to date object"""
        if not date_str or date_str == 'nan':
            return date.today()
        
        try:
            return datetime.strptime(date_str, '%m/%d/%Y').date()
        except ValueError:
            return date.today()
    
    def _extract_unit_of_measure(self, uom: str) -> str:
        """Extract and normalize unit of measure"""
        if not uom or str(uom) == 'nan':
            return 'unit'
        
        uom_lower = str(uom).lower()
        if 'oz' in uom_lower:
            return 'oz'
        elif 'ct' in uom_lower or 'count' in uom_lower:
            return 'ct'
        elif 'pack' in uom_lower:
            return 'pack'
        elif uom_lower == 'ca' or 'case' in uom_lower:
            return 'case'
        elif uom_lower == 'bo' or 'bottle' in uom_lower:
            return 'bottle'
        elif uom_lower == 'ea' or 'each' in uom_lower:
            return 'each'
        else:
            return 'unit'

