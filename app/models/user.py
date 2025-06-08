from sqlalchemy import Column, String, DateTime, Boolean
from sqlalchemy.sql import func
import uuid

from app.config.database import Base

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    phone_number = Column(String(20), unique=True, nullable=False, index=True)
    merchant_id = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    last_activity = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return f"<User {self.phone_number}>"