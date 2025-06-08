from sqlalchemy import Column, String, DateTime, Boolean, Text, JSON
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid

from app.config.database import Base

class Merchant(Base):
    __tablename__ = "merchants"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))  
    pine_labs_merchant_id = Column(String(100), unique=True, nullable=False, index=True)
    business_name = Column(String(255), nullable=False)
    business_type = Column(String(100))
    phone_number = Column(String(20), unique=True, index=True)
    email = Column(String(255))
    status = Column(String(50), default="ACTIVE")
    settings = Column(JSON, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    transactions = relationship("Transaction", back_populates="merchant")
    #settlements = relationship("Settlement", back_populates="merchant")
    #refunds = relationship("Refund", back_populates="merchant")
    #alerts = relationship("Alert", back_populates="merchant")
    
    def __repr__(self):
        return f"<Merchant {self.business_name}>"