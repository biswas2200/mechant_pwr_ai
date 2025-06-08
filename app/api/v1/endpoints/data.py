from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
import logging

from app.config.database import get_db
from app.config.settings import settings
from app.services.analytics_engine import AnalyticsEngine

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/debug")
async def debug_csv_data(db: Session = Depends(get_db)):
    """Debug endpoint to check CSV data loading"""
    
    try:
        analytics_engine = AnalyticsEngine(
            db=db, 
            use_csv=settings.USE_CSV_DATA, 
            csv_data_dir=settings.CSV_DATA_DIR
        )
        
        debug_info = analytics_engine.get_csv_debug_info()
        
        return {
            "settings": {
                "use_csv": settings.USE_CSV_DATA,
                "csv_dir": settings.CSV_DATA_DIR
            },
            "debug_info": debug_info
        }
        
    except Exception as e:
        logger.error(f"❌ CSV debug error: {e}")
        return {"error": str(e)}

@router.get("/summary")
async def get_data_summary(db: Session = Depends(get_db)):
    """Get summary of current data source"""
    
    try:
        analytics_engine = AnalyticsEngine(
            db=db, 
            use_csv=settings.USE_CSV_DATA, 
            csv_data_dir=settings.CSV_DATA_DIR
        )
        
        if settings.USE_CSV_DATA:
            summary = analytics_engine.get_csv_debug_info()
        else:
            # Database summary
            from app.models.merchant import Merchant
            merchant_count = db.query(Merchant).count()
            summary = {"merchants_count": merchant_count, "data_source": "Database"}
        
        return {
            "data_source": "CSV" if settings.USE_CSV_DATA else "Database",
            "settings": {
                "use_csv": settings.USE_CSV_DATA,
                "csv_dir": settings.CSV_DATA_DIR
            },
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"❌ Data summary error: {e}")
        return {"error": str(e)}

@router.get("/files")
async def check_csv_files():
    """Check what CSV files are available"""
    
    import os
    
    data_dir = settings.CSV_DATA_DIR
    files_found = []
    
    if os.path.exists(data_dir):
        for filename in os.listdir(data_dir):
            if filename.endswith('.csv'):
                filepath = os.path.join(data_dir, filename)
                file_size = os.path.getsize(filepath)
                files_found.append({
                    "filename": filename,
                    "size_bytes": file_size,
                    "size_mb": round(file_size / (1024 * 1024), 2)
                })
    
    return {
        "data_directory": data_dir,
        "directory_exists": os.path.exists(data_dir),
        "csv_files": files_found,
        "total_files": len(files_found)
    }