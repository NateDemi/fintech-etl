"""
Price Calculation Rules

This module contains business logic for handling price-related fields from CSV data:
- Extended Price (main price)
- Discount Adjustment Total
- Deposit Adjustment Total
- Miscellaneous Adjustment Total
- Tax Adjustment Total
- Delivery Adjustment Total
"""

import pandas as pd
from typing import Dict
from .base import BaseRule


class PriceRule(BaseRule):
    """Handles price-related calculations and adjustments from CSV data."""
    
    
    def get_extended_price(self, row: pd.Series) -> float:
        """Get Extended Price from CSV data (main price field)."""
        return self._num(row, "Extended Price", 0.0)
    
    def get_discount_amount(self, row: pd.Series) -> float:
        """Get Discount Adjustment Total from CSV data."""
        return self._num(row, "Discount Adjustment Total", 0.0)
    
    def get_deposit_amount(self, row: pd.Series) -> float:
        """Get Deposit Adjustment Total from CSV data."""
        return self._num(row, "DepositAdjustmentTotal", 0.0)
    
    def get_miscellaneous_amount(self, row: pd.Series) -> float:
        """Get Miscellaneous Adjustment Total from CSV data."""
        return self._num(row, "Miscellaneous Adjustment Total", 0.0)
    
    def get_tax_amount(self, row: pd.Series) -> float:
        """Get Tax Adjustment Total from CSV data."""
        return self._num(row, "Tax Adjustment Total", 0.0)
    
    def get_delivery_amount(self, row: pd.Series) -> float:
        """Get Delivery Adjustment Total from CSV data."""
        return self._num(row, "Delivery Adjustment Total", 0.0)
    
