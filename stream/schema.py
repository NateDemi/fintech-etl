from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from datetime import datetime

class LineItem(BaseModel):
    name: str = Field(..., description="Name and description of the item")
    qty: int = Field(..., description="Total units purchased")
    price: float = Field(..., description="Final listed price after discounts")
    discount: float = Field(default=0, description="Value of discount")
    upc: Optional[str] = Field(None, description="12-digit Universal Product Code")
    sku: Optional[str] = Field(None, description="SKU number")
    text: Optional[str] = Field(None, description="Original text segment")
    unitOfMeasure: str = Field(default="unit", description="Unit of measure")
    category: Optional[str] = Field(None, description="Product category")
    tax: float = Field(default=0, description="Tax amount for the item")
    notes: Optional[str] = Field(None, description="Additional notes")

class ProcessedReceipt(BaseModel):
    """Processed receipt ready for storage"""
    receipt_id: str
    vendor: str
    transaction_date: date
    total_amount: float
    sales_tax: float
    subtotal: float
    item_count: int
    line_items: List[LineItem]
    source_file: str
    processed_at: str
    gcs_bucket: str
    gcs_path: str
    document_id: str = "unknown"  # Email ID extracted from GCS path
