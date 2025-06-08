import re
from typing import Optional

def format_phone_number(phone: str) -> Optional[str]:
    """Format phone number to international format"""
    if not phone:
        return None
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Add country code if missing (assuming India +91)
    if len(digits) == 10:
        digits = "91" + digits
    elif len(digits) == 11 and digits.startswith("0"):
        digits = "91" + digits[1:]
    
    return f"+{digits}"

def format_currency(amount: float, currency: str = "INR") -> str:
    """Format currency amount"""
    if currency == "INR":
        return f"₹{amount:,.0f}"
    return f"{amount:,.2f} {currency}"

def calculate_percentage_change(current: float, previous: float) -> float:
    """Calculate percentage change between two values"""
    if previous == 0:
        return 0.0
    return ((current - previous) / previous) * 100