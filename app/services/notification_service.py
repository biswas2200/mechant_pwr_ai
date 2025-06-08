from twilio.rest import Client
from twilio.base.exceptions import TwilioException
import logging

from app.config.settings import settings

logger = logging.getLogger(__name__)

class NotificationService:
    def __init__(self):
        self.client = Client(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN)
        self.from_number = f"whatsapp:{settings.TWILIO_PHONE_NUMBER}"

    async def send_whatsapp_message(self, to_number: str, message: str) -> bool:
        """Send WhatsApp message via Twilio"""
        
        try:
            # Clean and format phone number
            clean_number = to_number.replace("whatsapp:", "").strip()
            
            # Add + if missing
            if not clean_number.startswith('+'):
                clean_number = f"+{clean_number}"
            
            # Format for WhatsApp
            formatted_number = f"whatsapp:{clean_number}"
            
            logger.info(f"Sending WhatsApp message to: {formatted_number}")
            
            # Format message for WhatsApp
            formatted_message = self._format_for_whatsapp(message)
            
            message_obj = self.client.messages.create(
                body=formatted_message,
                from_=self.from_number,
                to=formatted_number
            )
            
            logger.info(f"WhatsApp message sent successfully: {message_obj.sid}")
            return True
            
        except TwilioException as e:
            logger.error(f"Twilio error: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending WhatsApp: {e}")
            return False

    def _format_for_whatsapp(self, message: str) -> str:
        """Format message for better WhatsApp display"""
        
        # WhatsApp formatting
        formatted = message.replace("**", "*")  # Bold formatting
        formatted = formatted.replace("__", "_")  # Italic formatting
        
        # Ensure emojis are properly spaced
        formatted = formatted.replace("📊", "📊 ")
        formatted = formatted.replace("💡", "💡 ")
        formatted = formatted.replace("🎯", "🎯 ")
        formatted = formatted.replace("📈", "📈 ")
        formatted = formatted.replace("💰", "💰 ")
        formatted = formatted.replace("⚠️", "⚠️ ")
        formatted = formatted.replace("✅", "✅ ")
        
        # Remove extra spaces
        import re
        formatted = re.sub(r'\s+', ' ', formatted).strip()
        
        return formatted

    async def send_quick_reply_options(self, to_number: str, message: str, options: list) -> bool:
        """Send message with suggested quick replies"""
        
        # Add quick reply options to message
        options_text = "\n\n📋 *Quick Options:*\n"
        for i, option in enumerate(options, 1):
            options_text += f"{i}️⃣ {option}\n"
        
        full_message = message + options_text
        return await self.send_whatsapp_message(to_number, full_message)

    def format_business_summary(self, metrics: dict) -> str:
        """Format business metrics for WhatsApp"""
        
        today = metrics.get('today', {})
        yesterday = metrics.get('yesterday', {})
        
        # Calculate changes
        revenue_change = today.get('revenue', 0) - yesterday.get('revenue', 0)
        revenue_change_pct = (revenue_change / yesterday.get('revenue', 1)) * 100 if yesterday.get('revenue', 0) > 0 else 0
        
        change_emoji = "📈" if revenue_change >= 0 else "📉"
        change_text = f"+{revenue_change_pct:.1f}%" if revenue_change >= 0 else f"{revenue_change_pct:.1f}%"
        
        return f"""*📊 Business Summary*

*Today's Performance:*
💰 Revenue: ₹{today.get('revenue', 0):,.0f} {change_emoji} {change_text}
📈 Transactions: {today.get('transactions', 0)}
✅ Success Rate: {today.get('success_rate', 0):.1f}%
💳 Avg Amount: ₹{today.get('avg_amount', 0):,.0f}

*Yesterday:* ₹{yesterday.get('revenue', 0):,.0f} | {yesterday.get('transactions', 0)} txns

_Powered by MerchantGenius AI_ 🤖"""