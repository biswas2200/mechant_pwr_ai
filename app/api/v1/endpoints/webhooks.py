from fastapi import APIRouter, Form, Depends, BackgroundTasks
from fastapi.responses import PlainTextResponse
from sqlalchemy.orm import Session
import logging
import os

from app.config.database import get_db
from app.config.settings import settings
from app.services.ai_engine import MerchantAI
from app.services.analytics_engine import AnalyticsEngine
from app.services.notification_service import NotificationService
from app.models.merchant import Merchant
from app.models.user import User

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/whatsapp", response_class=PlainTextResponse)
async def whatsapp_webhook(
    background_tasks: BackgroundTasks,
    Body: str = Form(...),
    From: str = Form(...),
    db: Session = Depends(get_db)
):
    """Handle incoming WhatsApp messages"""
    
    try:
        # Extract phone number
        phone_number = From.replace("whatsapp:", "")
        
        # Find or create user
        user = db.query(User).filter(User.phone_number == phone_number).first()
        if not user:
            user = User(phone_number=phone_number)
            db.add(user)
            db.commit()
        
        # For CSV mode, use CSV merchant
        if settings.USE_CSV_DATA:
            merchant_id = "CSV_MERCHANT_001"
            merchant_name = "CSV Business"
        else:
            # Database mode
            merchant = db.query(Merchant).first()
            if not merchant:
                merchant = Merchant(
                    pine_labs_merchant_id="DEMO001",
                    business_name="Demo Business",
                    business_type="Retail",
                    phone_number=phone_number,
                    status="ACTIVE"
                )
                db.add(merchant)
                db.commit()
                user.merchant_id = merchant.id
                db.commit()
            merchant_id = merchant.id
            merchant_name = merchant.business_name
        
        # Initialize services with CSV support
        ai_engine = MerchantAI()
        analytics_engine = AnalyticsEngine(
            db=db, 
            use_csv=settings.USE_CSV_DATA, 
            csv_data_dir=settings.CSV_DATA_DIR
        )
        notification_service = NotificationService()
        
        # Process query
        logger.info(f"🔍 Processing query: '{Body}' for merchant: {merchant_name} (Data source: {'CSV' if settings.USE_CSV_DATA else 'Database'})")
        response = ai_engine.process_query(merchant_id, Body, analytics_engine)
        
        # LOG THE AI RESPONSE
        logger.info(f"🤖 AI RESPONSE GENERATED:")
        logger.info(f"==========================================")
        logger.info(f"{response}")
        logger.info(f"==========================================")
        
        # Try to send WhatsApp
        try:
            await notification_service.send_whatsapp_message(phone_number, response)
            logger.info("📱 WhatsApp message sent successfully")
        except Exception as e:
            logger.warning(f"📱 WhatsApp sending failed (expected during testing): {e}")
        
        logger.info(f"✅ Processed WhatsApp message from {phone_number}: {Body[:50]}...")
        
        return f"OK - AI Response: {response}"
        
    except Exception as e:
        logger.error(f"❌ WhatsApp webhook error: {e}")
        return f"ERROR: {str(e)}"

@router.post("/whatsapp/debug")
async def debug_ai_response(
    query: str = Form(...),
    db: Session = Depends(get_db)
):
    """Debug endpoint to test AI responses directly"""
    
    try:
        # Initialize analytics with CSV
        analytics_engine = AnalyticsEngine(
            db=db, 
            use_csv=settings.USE_CSV_DATA, 
            csv_data_dir=settings.CSV_DATA_DIR
        )
        
        # Get CSV debug info
        debug_info = analytics_engine.get_csv_debug_info() if settings.USE_CSV_DATA else {}
        
        ai_engine = MerchantAI()
        
        logger.info(f"🔍 DEBUG: Processing query: '{query}' (Data source: {'CSV' if settings.USE_CSV_DATA else 'Database'})")
        response = ai_engine.process_query("CSV_MERCHANT_001", query, analytics_engine)
        
        logger.info(f"🤖 DEBUG AI RESPONSE:")
        logger.info(f"==========================================")
        logger.info(f"{response}")
        logger.info(f"==========================================")
        
        return {
            "query": query,
            "merchant": "CSV Business" if settings.USE_CSV_DATA else "Database Business",
            "data_source": "CSV" if settings.USE_CSV_DATA else "Database",
            "ai_response": response,
            "debug_info": debug_info
        }
        
    except Exception as e:
        logger.error(f"❌ Debug error: {e}")
        return {"error": str(e)}

@router.get("/data/debug")
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

@router.get("/data/files")
async def check_csv_files():
    """Check what CSV files are available"""
    
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