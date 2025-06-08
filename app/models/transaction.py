from sqlalchemy import Column, String, DateTime, Float, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid

from app.config.database import Base

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    pine_labs_txn_id = Column(String(100), unique=True, nullable=False, index=True)
    merchant_id = Column(String, ForeignKey("merchants.id"), nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String(3), default="INR")
    payment_method = Column(String(50), nullable=False, index=True)
    status = Column(String(50), nullable=False, index=True)
    gateway_response_time = Column(Float)
    failure_reason = Column(String(255))
    customer_id = Column(String(100))
    order_id = Column(String(100))
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    merchant = relationship("Merchant", back_populates="transactions")

    def __repr__(self):
        return f"<Transaction {self.pine_labs_txn_id}: {self.amount} {self.currency}>"