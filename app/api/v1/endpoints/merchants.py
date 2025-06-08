from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from app.config.database import get_db
from app.models.merchant import Merchant
from app.schemas.merchant import Merchant as MerchantSchema, MerchantCreate

router = APIRouter()

@router.get("/", response_model=List[MerchantSchema])
def get_merchants(db: Session = Depends(get_db)):
    """Get all merchants"""
    merchants = db.query(Merchant).all()
    return merchants

@router.post("/", response_model=MerchantSchema)
def create_merchant(merchant: MerchantCreate, db: Session = Depends(get_db)):
    """Create a new merchant"""
    db_merchant = Merchant(**merchant.dict())
    db.add(db_merchant)
    db.commit()
    db.refresh(db_merchant)
    return db_merchant

@router.get("/{merchant_id}", response_model=MerchantSchema)
def get_merchant(merchant_id: str, db: Session = Depends(get_db)):
    """Get merchant by ID"""
    merchant = db.query(Merchant).filter(Merchant.id == merchant_id).first()
    if not merchant:
        raise HTTPException(status_code=404, detail="Merchant not found")
    return merchant