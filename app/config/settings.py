from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # App settings
    APP_NAME: str = "MerchantGenius AI"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True
    API_V1_STR: str = "/api/v1"
    
    # Data source configuration - ENABLE CSV
    USE_CSV_DATA: bool = True
    CSV_DATA_DIR: str = "data"
    
    # Database
    DATABASE_URL: str = "sqlite:///./merchantgenius.db"
    
    # External APIs
    OPENAI_API_KEY: str
    TWILIO_ACCOUNT_SID: str
    TWILIO_AUTH_TOKEN: str
    TWILIO_PHONE_NUMBER: str
    
    # Security
    SECRET_KEY: str = "your-secret-key-change-in-production"
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()