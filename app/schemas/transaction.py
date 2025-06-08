from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class TransactionBase(BaseModel):
    amount: float
    currency: str = "INR"
    payment_method: str
    status: str
    customer_id: Optional[str] = None
    order_id: Optional[str] = None

class TransactionCreate(TransactionBase):
    pine_labs_txn_id: str
    merchant_id: str

class Transaction(TransactionBase):
    id: str
    pine_labs_txn_id: str
    merchant_id: str
    gateway_response_time: Optional[float] = None
    failure_reason: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True