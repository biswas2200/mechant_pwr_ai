from typing import Generator
from sqlalchemy.orm import Session

from app.config.database import get_db

def get_database() -> Generator:
    """Dependency to get database session"""
    try:
        db = get_db()
        yield db
    finally:
        pass