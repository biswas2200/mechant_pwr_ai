from openai import OpenAI
from typing import Dict, Any
import json
import logging

from app.config.settings import settings
from app.services.analytics_engine import AnalyticsEngine

logger = logging.getLogger(__name__)

class MerchantAI:
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        
    def process_query(self, merchant_id: str, query: str, analytics_engine: AnalyticsEngine) -> str:
        """Process merchant query with AI"""
        
        try:
            # Step 1: Classify query intent
            intent = self._classify_intent(query)
            
            # Step 2: Gather relevant data
            context_data = self._gather_context_data(merchant_id, intent, analytics_engine)
            
            # Step 3: Generate AI response
            response = self._generate_response(query, intent, context_data)
            
            return response
            
        except Exception as e:
            logger.error(f"AI processing error: {e}")
            return "🤖 I'm having a moment. Could you try asking again? I'm here to help with your business insights!"

    def _classify_intent(self, query: str) -> str:
        """Simple rule-based intent classification"""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ['today', 'business', 'performance', 'how']):
            return 'BUSINESS_PULSE'
        elif any(word in query_lower for word in ['revenue', 'money', 'earning', 'sales', 'income']):
            return 'REVENUE_SUMMARY'
        elif any(word in query_lower for word in ['payment', 'method', 'upi', 'card', 'success rate']):
            return 'PAYMENT_ANALYSIS'
        elif any(word in query_lower for word in ['grow', 'opportunity', 'improve', 'optimize', 'increase']):
            return 'GROWTH_INSIGHTS'
        elif any(word in query_lower for word in ['help', 'what can you do', 'options', 'commands']):
            return 'HELP'
        elif any(word in query_lower for word in ['hi', 'hello', 'hey', 'start']):
            return 'GREETING'
        else:
            return 'GENERAL'

    def _gather_context_data(self, merchant_id: str, intent: str, analytics_engine: AnalyticsEngine) -> Dict[str, Any]:
        """Gather relevant data based on intent"""
        
        context = {}
        
        if intent in ['BUSINESS_PULSE', 'REVENUE_SUMMARY', 'PAYMENT_ANALYSIS']:
            context['business_metrics'] = analytics_engine.get_business_pulse(merchant_id)
            
        if intent == 'GROWTH_INSIGHTS':
            context['growth_insights'] = analytics_engine.get_growth_insights(merchant_id)
            context['business_metrics'] = analytics_engine.get_business_pulse(merchant_id)
        
        return context

    def _generate_response(self, query: str, intent: str, context_data: Dict[str, Any]) -> str:
        """Generate AI response optimized for WhatsApp"""
        
        if intent == 'GREETING':
            return """👋 *Hi! I'm MerchantGenius AI*

I help you understand your business better!

*Ask me things like:*
📊 "How's my business today?"
💰 "Show me my revenue"
💳 "Which payment method works best?"
📈 "What growth opportunities do I have?"
❓ "What can you help me with?"

*What would you like to know?* 🤔"""

        if intent == 'HELP':
            return """🤖 *MerchantGenius AI Help*

*I can help you with:*

📊 *Business Performance*
- Daily/weekly summaries
- Transaction success rates
- Revenue analysis

💳 *Payment Analytics*
- Payment method performance
- Success rate comparison
- Failure analysis

📈 *Growth Insights*
- Optimization opportunities
- Customer behavior patterns
- Revenue improvement tips

*Just ask me naturally!* 
Example: "How's my UPI performance today?"

*What would you like to explore?* 💡"""

        system_prompt = """
        You are MerchantGenius AI, a WhatsApp business assistant for merchants.
        
        IMPORTANT GUIDELINES:
        - Keep responses SHORT and WhatsApp-friendly (under 200 words)
        - Use emojis appropriately but don't overdo it
        - Format for mobile reading with line breaks
        - Be conversational and helpful
        - Always provide actionable insights
        - Use *bold* for important numbers and headings
        
        Response structure for business queries:
        *📊 Key Insight* 
        [Main finding in 1-2 lines]
        
        *💡 What this means*
        [Business interpretation]
        
        *🎯 Action Item*
        [Specific recommendation]
        """
        
        user_prompt = f"""
        Merchant Query: {query}
        Intent: {intent}
        Business Data: {json.dumps(context_data, indent=2, default=str)}
        
        Provide a WhatsApp-friendly response that directly answers their question.
        Keep it concise but valuable.
        """
        
        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                max_tokens=300,
                temperature=0.3
            )
            
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return self._fallback_response(intent, context_data)

    def _fallback_response(self, intent: str, context_data: Dict[str, Any]) -> str:
        """Fallback response when AI fails"""
        
        if intent == 'BUSINESS_PULSE' and 'business_metrics' in context_data:
            metrics = context_data['business_metrics']['today']
            return f"""*📊 Today's Performance*

💰 Revenue: ₹{metrics['revenue']:,.0f}
📈 Transactions: {metrics['transactions']}
✅ Success Rate: {metrics['success_rate']}%

*🎯 Looking good!* Keep up the great work! 

_Ask me for more details anytime_ 😊"""
        
        return "📊 I can help you with business insights, revenue analysis, and growth opportunities. What specific information would you like to know?"