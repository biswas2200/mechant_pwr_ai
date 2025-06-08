from fastapi import APIRouter

from app.api.v1.endpoints import webhooks, merchants, analytics

api_router = APIRouter()

api_router.include_router(webhooks.router, prefix="/webhooks", tags=["webhooks"])
api_router.include_router(merchants.router, prefix="/merchants", tags=["merchants"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
# api_router.include_router(data.router, prefix="/data", tags=["data"])