from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime

class MerchantBase(BaseModel):
    business_name: str
    business_type: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None

class MerchantCreate(MerchantBase):
    pine_labs_merchant_id: str

class MerchantUpdate(BaseModel):
    business_name: Optional[str] = None
    business_type: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None

class Merchant(MerchantBase):
    id: str
    pine_labs_merchant_id: str
    status: str
    settings: Dict[str, Any]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True