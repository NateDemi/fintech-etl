import pandas as pd
from typing import Dict, Set
from .base import BaseRule


class QuantityRule(BaseRule):
    """Handles quantity calculation logic for different product categories."""
    
    def __init__(self, item_rule=None):
        self.item_rule = item_rule
    
    SPECIAL_PACK_SIZES: Dict[str, Set[int]] = {
        BaseRule.BEER: {12, 24},
        BaseRule.WINE: set(),  # Wine only uses packs per case
        BaseRule.SPIRITS: set(),
        BaseRule.NON_ALC: set(),
        BaseRule.MISC: set(),
    }
    
    def calculate_quantity(self, row: pd.Series) -> int:
        """
        Main quantity calculation router - delegates to category-specific functions.
        """
        qty = self._num(row, "Quantity", 0)
        if qty <= 0:
            return 0

        uom = self._extract_unit_of_measure(row.get("Unit Of Measure", ""))
        if uom == "bottle":  # bottle → take quantity as-is
            return int(qty)

        category = self._identify_product_category(row)
        
        # Route to category-specific quantity calculation
        if category == self.BEER:
            return self._beer_quantity(row)
        elif category == self.WINE:
            return self._wine_quantity(row)
        elif category == self.SPIRITS:
            return self._spirits_quantity(row)
        elif category == self.NON_ALC:
            return self._non_alcoholic_quantity(row)
        elif category == self.MISC:
            return self._miscellaneous_quantity(row)
        else:
            # Default calculation for unknown categories
            packs = self._get_packs_per_case(row)
            return int(qty * packs)
    
    def _beer_quantity(self, row: pd.Series) -> int:
        """Calculate quantity specifically for beer items."""
        qty = self._num(row, "Quantity", 0)
        if qty <= 0:
            return 0
        
        packs = self._get_packs_per_case(row)
        
        # Beer special rule: if 12 or 24 packs per case, multiply by Units Per Pack
        if int(packs) in self.SPECIAL_PACK_SIZES[self.BEER]:
            if self.item_rule:
                units = self.item_rule.get_units_per_pack(row)
            else:
                units = self._num(row, "Units Per Pack", 1) or 1
            return int(qty * packs * units)
        
        # Standard beer calculation
        return int(qty * packs)
    
    def _wine_quantity(self, row: pd.Series) -> int:
        """Calculate quantity specifically for wine items - just multiply by packs per case."""
        qty = self._num(row, "Quantity", 0)
        if qty <= 0:
            return 0
        
        packs = self._get_packs_per_case(row)
        return int(qty * packs)
    
    def _spirits_quantity(self, row: pd.Series) -> int:
        """Calculate quantity specifically for spirits items."""
        qty = self._num(row, "Quantity", 0)
        if qty <= 0:
            return 0
        
        packs = self._get_packs_per_case(row)
        return int(qty * packs)
    
    def _non_alcoholic_quantity(self, row: pd.Series) -> int:
        """Calculate quantity specifically for non-alcoholic items."""
        qty = self._num(row, "Quantity", 0)
        if qty <= 0:
            return 0
        
        packs = self._get_packs_per_case(row)
        return int(qty * packs)
    
    def _miscellaneous_quantity(self, row: pd.Series) -> int:
        """Calculate quantity specifically for miscellaneous items."""
        qty = self._num(row, "Quantity", 0)
        if qty <= 0:
            return 0
        
        packs = self._get_packs_per_case(row)
        return int(qty * packs)
    
    def _get_packs_per_case(self, row: pd.Series) -> int:
        """Get Packs Per Case from CSV data."""
        return int(self._num(row, "Packs Per Case", 1) or 1)
    
    
