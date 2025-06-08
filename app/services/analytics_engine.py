import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
import logging

from app.models.transaction import Transaction
from app.models.merchant import Merchant
from app.services.csv_data_service import CSVDataService

logger = logging.getLogger(__name__)

class AnalyticsEngine:
    def __init__(self, db: Session, use_csv: bool = True, csv_data_dir: str = "data"):
        self.db = db
        self.use_csv = use_csv
        
        if self.use_csv:
            self.csv_service = CSVDataService(csv_data_dir)
            logger.info("📊 Analytics Engine initialized with CSV data source")
        else:
            logger.info("📊 Analytics Engine initialized with database source")

    def get_business_pulse(self, merchant_id: str = None) -> Dict[str, Any]:
        """Get business metrics from CSV or database"""
        
        if self.use_csv:
            return self._get_pulse_from_csv()
        else:
            return self._get_pulse_from_db(merchant_id)
    
    def _get_pulse_from_csv(self) -> Dict[str, Any]:
        """Get business pulse from CSV data"""
        
        logger.info("📈 Getting business pulse from CSV data...")
        
        # Get transactions data
        df = self.csv_service.get_transactions(days=30)
        
        if df.empty:
            logger.warning("❌ No CSV transaction data found!")
            return self._empty_pulse_response()
        
        logger.info(f"📊 Analyzing {len(df)} transactions from CSV")
        
        # Today's metrics
        today = datetime.now().date().isoformat()
        yesterday = (datetime.now().date() - timedelta(days=1)).isoformat()
        
        today_data = df[df['date'] == today] if 'date' in df.columns else pd.DataFrame()
        yesterday_data = df[df['date'] == yesterday] if 'date' in df.columns else pd.DataFrame()
        
        # If no today data, get recent data for demo
        if today_data.empty:
            logger.info("No today data, using recent data for demo")
            recent_data = df.tail(50)  # Get last 50 transactions
            today_data = recent_data
        
        metrics = {
            'today': self._calculate_day_metrics(today_data),
            'yesterday': self._calculate_day_metrics(yesterday_data),
            'payment_methods': self._analyze_payment_methods(df),
            'recent_trends': self._calculate_trends(df),
            'data_source': 'CSV',
            'total_records': len(df),
            'csv_summary': self.csv_service.get_data_summary()
        }
        
        logger.info(f"✅ Business pulse calculated successfully")
        
        return metrics
    
    def _get_pulse_from_db(self, merchant_id: str) -> Dict[str, Any]:
        """Get business pulse from database (fallback)"""
        
        transactions = self.db.query(Transaction).filter(
            Transaction.merchant_id == merchant_id
        ).order_by(Transaction.created_at.desc()).limit(1000).all()
        
        if not transactions:
            logger.warning("❌ No database transactions found!")
            return self._empty_pulse_response()
        
        # Convert to DataFrame
        data = []
        for txn in transactions:
            data.append({
                'amount': txn.amount,
                'payment_method': txn.payment_method,
                'status': txn.status,
                'created_at': txn.created_at,
                'date': txn.created_at.date().isoformat()
            })
        
        df = pd.DataFrame(data)
        
        today = datetime.now().date().isoformat()
        yesterday = (datetime.now().date() - timedelta(days=1)).isoformat()
        
        today_data = df[df['date'] == today]
        yesterday_data = df[df['date'] == yesterday]
        
        metrics = {
            'today': self._calculate_day_metrics(today_data),
            'yesterday': self._calculate_day_metrics(yesterday_data),
            'payment_methods': self._analyze_payment_methods(df),
            'recent_trends': self._calculate_trends(df),
            'data_source': 'Database',
            'total_records': len(df)
        }
        
        return metrics

    def _calculate_day_metrics(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate metrics for a specific day"""
        if data.empty:
            return {'revenue': 0, 'transactions': 0, 'success_rate': 0, 'avg_amount': 0}
        
        successful = data[data['status'] == 'SUCCESS'] if 'status' in data.columns else pd.DataFrame()
        
        return {
            'revenue': float(successful['amount'].sum()) if not successful.empty and 'amount' in successful.columns else 0,
            'transactions': len(data),
            'success_rate': round((len(successful) / len(data)) * 100, 2) if len(data) > 0 else 0,
            'avg_amount': float(successful['amount'].mean()) if not successful.empty and 'amount' in successful.columns else 0
        }

    def _analyze_payment_methods(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Analyze payment method performance"""
        if data.empty or 'payment_method' not in data.columns:
            return {}
        
        method_stats = {}
        for method in data['payment_method'].unique():
            if pd.isna(method):
                continue
                
            method_data = data[data['payment_method'] == method]
            method_successful = method_data[method_data['status'] == 'SUCCESS'] if 'status' in method_data.columns else pd.DataFrame()
            
            method_stats[str(method)] = {
                'total_revenue': float(method_successful['amount'].sum()) if not method_successful.empty and 'amount' in method_successful.columns else 0,
                'transaction_count': len(method_data),
                'success_rate': round((len(method_successful) / len(method_data)) * 100, 2) if len(method_data) > 0 else 0,
                'avg_amount': float(method_successful['amount'].mean()) if not method_successful.empty and 'amount' in method_successful.columns else 0
            }
        
        return method_stats

    def _calculate_trends(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate basic trends"""
        if data.empty:
            return {}
        
        trends = {}
        
        # Daily revenue trend (last 7 days)
        if 'date' in data.columns and 'status' in data.columns and 'amount' in data.columns:
            recent_week = data[data['date'] >= (datetime.now().date() - timedelta(days=7)).isoformat()]
            if not recent_week.empty:
                daily_revenue = recent_week[recent_week['status'] == 'SUCCESS'].groupby('date')['amount'].sum()
                trends['daily_revenue_trend'] = daily_revenue.to_dict() if not daily_revenue.empty else {}
        
        # Trending payment method
        if 'payment_method' in data.columns:
            payment_counts = data['payment_method'].value_counts()
            trends['trending_payment_method'] = str(payment_counts.index[0]) if len(payment_counts) > 0 else None
        
        return trends

    def _empty_pulse_response(self) -> Dict[str, Any]:
        """Return empty response when no data"""
        return {
            'today': {'revenue': 0, 'transactions': 0, 'success_rate': 0, 'avg_amount': 0},
            'yesterday': {'revenue': 0, 'transactions': 0, 'success_rate': 0, 'avg_amount': 0},
            'payment_methods': {},
            'recent_trends': {},
            'data_source': 'CSV' if self.use_csv else 'Database',
            'total_records': 0,
            'message': 'No data found - check CSV files in data/ directory'
        }

    def get_growth_insights(self, merchant_id: str = None) -> List[Dict[str, Any]]:
        """Get growth insights"""
        
        if self.use_csv:
            df = self.csv_service.get_transactions(days=30)
        else:
            # Database logic
            transactions = self.db.query(Transaction).filter(
                Transaction.merchant_id == merchant_id
            ).order_by(Transaction.created_at.desc()).limit(500).all()
            
            data = []
            for txn in transactions:
                data.append({
                    'amount': txn.amount,
                    'payment_method': txn.payment_method,
                    'status': txn.status
                })
            df = pd.DataFrame(data)
        
        if df.empty:
            return []
        
        insights = []
        
        # High-value transaction insight
        if 'amount' in df.columns and 'status' in df.columns:
            high_value_threshold = 5000
            high_value_txns = df[df['amount'] >= high_value_threshold]
            
            if len(high_value_txns) > 0:
                failure_rate = (high_value_txns['status'] != 'SUCCESS').mean()
                if failure_rate > 0.1:
                    insights.append({
                        'type': 'HIGH_VALUE_FAILURES',
                        'title': 'High-Value Transaction Issues',
                        'description': f'{failure_rate:.1%} of transactions above ₹{high_value_threshold} are failing',
                        'recommendation': 'Consider enabling EMI or alternative payment methods for high-value orders'
                    })
        
        # Payment method optimization
        if 'payment_method' in df.columns and 'status' in df.columns and not df.empty:
            method_performance = df.groupby('payment_method').apply(
                lambda x: (x['status'] == 'SUCCESS').mean()
            ).sort_values(ascending=False)
            
            if len(method_performance) > 1:
                best_method = method_performance.index[0]
                best_rate = method_performance.iloc[0]
                worst_method = method_performance.index[-1]
                worst_rate = method_performance.iloc[-1]
                
                if best_rate - worst_rate > 0.1:
                    insights.append({
                        'type': 'PAYMENT_METHOD_OPTIMIZATION',
                        'title': 'Payment Method Performance Gap',
                        'description': f'{best_method} has {best_rate:.1%} success rate vs {worst_method} at {worst_rate:.1%}',
                        'recommendation': f'Promote {best_method} as the preferred payment method'
                    })
        
        return insights
    
    def get_csv_debug_info(self) -> Dict[str, Any]:
        """Get debug information about CSV data"""
        if not self.use_csv:
            return {"message": "Not using CSV data"}
        
        return {
            "csv_summary": self.csv_service.get_data_summary(),
            "merchant_names": self.csv_service.get_merchant_names(),
            "transactions_sample": self.csv_service.transactions_df.head().to_dict() if not self.csv_service.transactions_df.empty else {},
            "columns": list(self.csv_service.transactions_df.columns) if not self.csv_service.transactions_df.empty else []
        }