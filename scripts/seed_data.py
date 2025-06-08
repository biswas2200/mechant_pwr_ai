#!/usr/bin/env python3

import sys
import os
from datetime import datetime, timedelta
import random

# Add the app directory to the path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.config.database import SessionLocal, engine, Base
from app.models.merchant import Merchant
from app.models.transaction import Transaction

def create_sample_data():
    """Create sample data for testing"""
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    db = SessionLocal()
    
    try:
        # Create sample merchant
        merchant = Merchant(
            pine_labs_merchant_id="DEMO001",
            business_name="Demo Fashion Store",
            business_type="Fashion Retail",
            phone_number="+919876543210",
            email="demo@store.com",
            status="ACTIVE"
        )
        
        db.add(merchant)
        db.commit()
        db.refresh(merchant)
        
        print(f"Created merchant: {merchant.business_name}")
        
        # Create sample transactions
        payment_methods = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING"]
        statuses = ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS", "FAILED"]  # 80% success rate
        
        for i in range(100):
            # Random date in the last 30 days
            days_ago = random.randint(0, 30)
            txn_date = datetime.now() - timedelta(days=days_ago)
            
            transaction = Transaction(
                pine_labs_txn_id=f"TXN{1000 + i}",
                merchant_id=merchant.id,
                amount=random.uniform(100, 5000),
                payment_method=random.choice(payment_methods),
                status=random.choice(statuses),
                gateway_response_time=random.uniform(0.5, 3.0),
                customer_id=f"CUST{random.randint(1000, 9999)}",
                order_id=f"ORD{random.randint(10000, 99999)}",
                created_at=txn_date
            )
            
            db.add(transaction)
        
        db.commit()
        print("Created 100 sample transactions")
        
        print("\n✅ Sample data created successfully!")
        print(f"Merchant ID: {merchant.id}")
        print("You can now test the WhatsApp integration")
        
    except Exception as e:
        print(f"Error creating sample data: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    create_sample_data()