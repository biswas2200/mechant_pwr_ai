from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from typing import Dict, Any

from app.config.database import get_db
from app.services.analytics_engine import AnalyticsEngine

router = APIRouter()

@router.get("/pulse/{merchant_id}")
def get_business_pulse(merchant_id: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Get business pulse for a merchant"""
    analytics = AnalyticsEngine(db)
    return analytics.get_business_pulse(merchant_id)

@router.get("/insights/{merchant_id}")
def get_growth_insights(merchant_id: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    """Get growth insights for a merchant"""
    analytics = AnalyticsEngine(db)
    insights = analytics.get_growth_insights(merchant_id)
    return {"insights": insights}