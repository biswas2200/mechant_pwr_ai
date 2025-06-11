import pandas as pd
import os
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class CSVDataService:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.transactions_df = None
        self.settlements_df = None
        self.support_df = None
        logger.info(f"🔍 Initializing CSV service with directory: {data_dir}")
        self._load_csv_files()
    
    def _load_csv_files(self):
        """Load CSV files into memory"""
        logger.info(f"🔍 Looking for CSV files in: {self.data_dir}")
        
        if not os.path.exists(self.data_dir):
            logger.error(f"❌ Data directory does not exist: {self.data_dir}")
            self.transactions_df = pd.DataFrame()
            return
        
        # List all files
        all_files = os.listdir(self.data_dir)
        csv_files = [f for f in all_files if f.endswith('.csv')]
        logger.info(f"📊 CSV files found: {csv_files}")
        
        # Load transactions from txn_refunds.csv
        self._load_transactions()
        
        # Load settlements from settlement_data.csv  
        self._load_settlements()
        
        # Load support data from Support Data(Sheet1).csv
        self._load_support_data()
    
    def _load_transactions(self):
        """Load transactions from txn_refunds.csv"""
        transaction_file = os.path.join(self.data_dir, "txn_refunds.csv")
        
        if os.path.exists(transaction_file):
            logger.info(f"📥 Found transactions file: {transaction_file}")
            try:
                logger.info(f"📊 Reading transactions CSV file...")
                
                # Try different encodings
                encodings = ['utf-8', 'latin-1', 'cp1252']
                
                for encoding in encodings:
                    try:
                        self.transactions_df = pd.read_csv(transaction_file, encoding=encoding)
                        logger.info(f"✅ Successfully loaded transactions with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        logger.warning(f"⚠️ Failed with {encoding} encoding, trying next...")
                        continue
                
                if self.transactions_df is None:
                    logger.error("❌ Failed to read transactions CSV with any encoding")
                    self.transactions_df = pd.DataFrame()
                    return
                
                logger.info(f"✅ Loaded {len(self.transactions_df)} rows from txn_refunds.csv")
                logger.info(f"📋 Columns found: {list(self.transactions_df.columns)}")
                
                # Clean the transaction data
                self._clean_transaction_data()
                
                logger.info(f"🎉 Transactions processed: {len(self.transactions_df)} ready for analysis")
                
            except Exception as e:
                logger.error(f"❌ Error reading txn_refunds.csv: {e}")
                self.transactions_df = pd.DataFrame()
        else:
            logger.warning("❌ txn_refunds.csv not found!")
            self.transactions_df = pd.DataFrame()
    
    def _load_settlements(self):
        """Load settlements from settlement_data.csv"""
        settlement_file = os.path.join(self.data_dir, "settlement_data.csv")
        
        if os.path.exists(settlement_file):
            logger.info(f"📥 Found settlements file: {settlement_file}")
            try:
                # Try different encodings
                encodings = ['utf-8', 'latin-1', 'cp1252']
                
                for encoding in encodings:
                    try:
                        self.settlements_df = pd.read_csv(settlement_file, encoding=encoding)
                        logger.info(f"✅ Successfully loaded settlements with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if self.settlements_df is not None:
                    logger.info(f"✅ Loaded {len(self.settlements_df)} settlement records")
                    logger.info(f"📋 Settlement columns: {list(self.settlements_df.columns)}")
                    self._clean_settlement_data()
                
            except Exception as e:
                logger.error(f"❌ Error reading settlement_data.csv: {e}")
                self.settlements_df = pd.DataFrame()
        else:
            logger.warning("❌ settlement_data.csv not found!")
            self.settlements_df = pd.DataFrame()
    
    def _load_support_data(self):
        """Load support data from Support Data(Sheet1).csv"""
        support_file = os.path.join(self.data_dir, "Support Data(Sheet1).csv")
        
        if os.path.exists(support_file):
            logger.info(f"📥 Found support file: {support_file}")
            try:
                # Try different encodings
                encodings = ['utf-8', 'latin-1', 'cp1252']
                
                for encoding in encodings:
                    try:
                        self.support_df = pd.read_csv(support_file, encoding=encoding)
                        logger.info(f"✅ Successfully loaded support data with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if self.support_df is not None:
                    logger.info(f"✅ Loaded {len(self.support_df)} support records")
                    logger.info(f"📋 Support columns: {list(self.support_df.columns)}")
                    self._clean_support_data()
                
            except Exception as e:
                logger.error(f"❌ Error reading Support Data(Sheet1).csv: {e}")
                self.support_df = pd.DataFrame()
        else:
            logger.warning("❌ Support Data(Sheet1).csv not found!")
            self.support_df = pd.DataFrame()
    
    def _clean_transaction_data(self):
        """Clean transaction data by dropping nulls and bad data"""
        if self.transactions_df.empty:
            return
        
        logger.info("🧹 Cleaning transaction data...")
        original_count = len(self.transactions_df)
        logger.info(f"📋 Original columns: {list(self.transactions_df.columns)}")
        logger.info(f"📊 Original row count: {original_count}")
        
        # Drop columns that are mostly empty (>80% null)
        null_threshold = 0.8
        null_percentages = self.transactions_df.isnull().sum() / len(self.transactions_df)
        columns_to_drop = null_percentages[null_percentages > null_threshold].index.tolist()
        
        if columns_to_drop:
            logger.info(f"🗑️ Dropping mostly empty columns: {columns_to_drop}")
            self.transactions_df = self.transactions_df.drop(columns=columns_to_drop)
        
        # Updated column mapping based on actual txn_refunds.csv structure
        column_mapping = {
            'transaction_id': 'txn_id',
            'merchant_display_name': 'merchant_name',
            'txn_status_name': 'status',
            'payment_mode_name': 'payment_method',
            'transaction_start_date_time': 'created_at',
            'acquirer_name': 'gateway',
            'amount': 'amount',
            'txn_completion_date_time': 'completed_at',
            'transaction_type_name': 'transaction_type',
            'category': 'category',
        }
        
        # Apply mapping
        for old_col, new_col in column_mapping.items():
            if old_col in self.transactions_df.columns and new_col not in self.transactions_df.columns:
                self.transactions_df[new_col] = self.transactions_df[old_col]
                logger.info(f"✅ Mapped {old_col} → {new_col}")
        
        # Critical columns that MUST have values
        critical_columns = ['transaction_id', 'amount', 'merchant_display_name', 'transaction_start_date_time']
        
        # Drop rows missing critical data
        for col in critical_columns:
            if col in self.transactions_df.columns:
                before_count = len(self.transactions_df)
                self.transactions_df = self.transactions_df.dropna(subset=[col])
                after_count = len(self.transactions_df)
                if before_count != after_count:
                    logger.info(f"🗑️ Dropped {before_count - after_count} rows missing {col}")
        
        # Clean and validate amount
        if 'amount' in self.transactions_df.columns:
            logger.info("💰 Processing amounts...")
            
            # Convert to numeric, invalid values become NaN
            self.transactions_df['amount'] = pd.to_numeric(self.transactions_df['amount'], errors='coerce')
            
            # Drop rows with invalid amounts
            before_count = len(self.transactions_df)
            self.transactions_df = self.transactions_df.dropna(subset=['amount'])
            after_count = len(self.transactions_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} rows with invalid amounts")
            
            # Drop rows with zero or negative amounts
            before_count = len(self.transactions_df)
            self.transactions_df = self.transactions_df[self.transactions_df['amount'] > 0]
            after_count = len(self.transactions_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} rows with zero/negative amounts")
            
            # Remove extreme outliers (amounts > 1 crore)
            before_count = len(self.transactions_df)
            self.transactions_df = self.transactions_df[self.transactions_df['amount'] <= 10000000]
            after_count = len(self.transactions_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} rows with extreme amounts")
            
            # Check if amounts are in paise
            median_amount = self.transactions_df['amount'].median()
            logger.info(f"💰 Median amount: {median_amount}")
            
            # Convert from paise if needed
            has_convenience_paise = 'convenience_fees_amt_in_paise' in self.transactions_df.columns
            if has_convenience_paise and median_amount > 10000:
                logger.info("💰 Converting amounts from paise to rupees")
                self.transactions_df['amount'] = self.transactions_df['amount'] / 100
            
            logger.info(f"💰 Final amount range: ₹{self.transactions_df['amount'].min():.2f} to ₹{self.transactions_df['amount'].max():.2f}")
        
        # Clean and validate dates
        date_columns = ['transaction_start_date_time', 'created_at', 'sale_txn_date_time', 'txn_completion_date_time']
        date_found = False
        
        for date_col in date_columns:
            if date_col in self.transactions_df.columns:
                logger.info(f"📅 Processing date column: {date_col}")
                
                # Convert to datetime
                self.transactions_df['created_at'] = pd.to_datetime(self.transactions_df[date_col], errors='coerce')
                
                # Drop rows with invalid dates
                before_count = len(self.transactions_df)
                self.transactions_df = self.transactions_df.dropna(subset=['created_at'])
                after_count = len(self.transactions_df)
                if before_count != after_count:
                    logger.info(f"🗑️ Dropped {before_count - after_count} rows with invalid dates")
                
                # Drop future dates (likely data errors)
                before_count = len(self.transactions_df)
                self.transactions_df = self.transactions_df[self.transactions_df['created_at'] <= datetime.now()]
                after_count = len(self.transactions_df)
                if before_count != after_count:
                    logger.info(f"🗑️ Dropped {before_count - after_count} rows with future dates")
                
                # Create date string for easier filtering
                self.transactions_df['date'] = self.transactions_df['created_at'].dt.date.astype(str)
                
                if len(self.transactions_df) > 0:
                    logger.info(f"📅 Date range: {self.transactions_df['created_at'].min()} to {self.transactions_df['created_at'].max()}")
                    date_found = True
                    break
        
        if not date_found:
            logger.warning("📅 No valid date column found - this will limit analytics")
        
        # Clean status
        if 'txn_status_name' in self.transactions_df.columns:
            self.transactions_df['status'] = self.transactions_df['txn_status_name']
        
        if 'status' in self.transactions_df.columns:
            logger.info("✅ Processing transaction statuses...")
            
            # Drop rows with null status
            before_count = len(self.transactions_df)
            self.transactions_df = self.transactions_df.dropna(subset=['status'])
            after_count = len(self.transactions_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} rows with missing status")
            
            # Standardize status values
            status_mapping = {
                'SUCCESS': 'SUCCESS', 'SUCCESSFUL': 'SUCCESS', 'CAPTURED': 'SUCCESS',
                'SETTLED': 'SUCCESS', 'COMPLETED': 'SUCCESS', 'APPROVED': 'SUCCESS',
                'FAILED': 'FAILED', 'FAILURE': 'FAILED', 'DECLINED': 'FAILED',
                'TIMEOUT': 'FAILED', 'ERROR': 'FAILED', 'REJECTED': 'FAILED',
                'PENDING': 'PENDING', 'INITIATED': 'PENDING', 'PROCESSING': 'PENDING'
            }
            
            self.transactions_df['status'] = (
                self.transactions_df['status'].astype(str).str.upper()
                .map(status_mapping)
            )
            
            # Drop rows with unmapped status
            before_count = len(self.transactions_df)
            self.transactions_df = self.transactions_df.dropna(subset=['status'])
            after_count = len(self.transactions_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} rows with unknown status")
            
            final_statuses = self.transactions_df['status'].value_counts()
            logger.info(f"✅ Final status distribution: {final_statuses.to_dict()}")
        
        # Clean payment methods
        if 'payment_mode_name' in self.transactions_df.columns:
            self.transactions_df['payment_method'] = self.transactions_df['payment_mode_name']
        
        if 'payment_method' in self.transactions_df.columns:
            logger.info("💳 Processing payment methods...")
            
            # Drop rows with null payment method
            before_count = len(self.transactions_df)
            self.transactions_df = self.transactions_df.dropna(subset=['payment_method'])
            after_count = len(self.transactions_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} rows with missing payment method")
            
            # Standardize payment methods
            payment_mapping = {
                'CREDIT CARD': 'CREDIT_CARD', 'DEBIT CARD': 'DEBIT_CARD',
                'UPI': 'UPI', 'NET BANKING': 'NET_BANKING',
                'WALLET': 'WALLET', 'EMI': 'EMI',
                'CREDIT_CARD': 'CREDIT_CARD', 'DEBIT_CARD': 'DEBIT_CARD'
            }
            
            self.transactions_df['payment_method'] = (
                self.transactions_df['payment_method'].astype(str).str.upper()
                .map(payment_mapping)
            )
            
            # Keep unmapped methods as-is (don't drop them)
            self.transactions_df['payment_method'] = self.transactions_df['payment_method'].fillna(
                self.transactions_df['payment_mode_name'].astype(str).str.upper()
            )
            
            final_methods = self.transactions_df['payment_method'].value_counts()
            logger.info(f"💳 Final payment methods: {final_methods.to_dict()}")
        
        # Clean merchant names
        if 'merchant_display_name' in self.transactions_df.columns:
            # Drop rows with null merchant names
            before_count = len(self.transactions_df)
            self.transactions_df = self.transactions_df.dropna(subset=['merchant_display_name'])
            after_count = len(self.transactions_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} rows with missing merchant name")
            
            self.transactions_df['merchant_name'] = self.transactions_df['merchant_display_name']
            self.transactions_df['merchant_id'] = 'CSV_MERCHANT_001'
        
        # Final data quality check
        final_count = len(self.transactions_df)
        dropped_count = original_count - final_count
        drop_percentage = (dropped_count / original_count) * 100
        
        logger.info(f"🧹 Data cleaning complete!")
        logger.info(f"📊 Original: {original_count} rows")
        logger.info(f"📊 Final: {final_count} rows")
        logger.info(f"🗑️ Dropped: {dropped_count} rows ({drop_percentage:.1f}%)")
        
        if final_count > 0:
            unique_merchants = self.transactions_df['merchant_name'].nunique()
            logger.info(f"👥 Unique merchants: {unique_merchants}")
            
            if unique_merchants <= 5:
                merchant_names = list(self.transactions_df['merchant_name'].unique())
                logger.info(f"👥 Merchant names: {merchant_names}")
        else:
            logger.error("❌ No valid transactions remaining after cleaning!")
    
    def _clean_settlement_data(self):
        """Clean settlement data by dropping nulls"""
        if self.settlements_df.empty:
            return
        
        logger.info("🧹 Cleaning settlement data...")
        original_count = len(self.settlements_df)
        
        # Drop mostly empty columns
        null_threshold = 0.8
        null_percentages = self.settlements_df.isnull().sum() / len(self.settlements_df)
        columns_to_drop = null_percentages[null_percentages > null_threshold].index.tolist()
        
        if columns_to_drop:
            logger.info(f"🗑️ Dropping mostly empty settlement columns: {columns_to_drop}")
            self.settlements_df = self.settlements_df.drop(columns=columns_to_drop)
        
        # Clean settlement amounts
        amount_columns = ['amount', 'settlement_amount', 'actual_txn_amount', 'refund_amount']
        for col in amount_columns:
            if col in self.settlements_df.columns:
                # Convert to numeric and drop invalid values
                self.settlements_df[col] = pd.to_numeric(self.settlements_df[col], errors='coerce')
                before_count = len(self.settlements_df)
                self.settlements_df = self.settlements_df.dropna(subset=[col])
                after_count = len(self.settlements_df)
                if before_count != after_count:
                    logger.info(f"🗑️ Dropped {before_count - after_count} settlements with invalid {col}")
                
                # Convert from paise if needed
                median_val = self.settlements_df[col].median()
                if median_val > 10000:  # Likely in paise
                    logger.info(f"💰 Converting {col} from paise to rupees")
                    self.settlements_df[col] = self.settlements_df[col] / 100
        
        # Clean settlement dates
        if 'transaction_start_date_time' in self.settlements_df.columns:
            self.settlements_df['settlement_date'] = pd.to_datetime(
                self.settlements_df['transaction_start_date_time'], errors='coerce'
            )
            # Drop rows with invalid dates
            before_count = len(self.settlements_df)
            self.settlements_df = self.settlements_df.dropna(subset=['settlement_date'])
            after_count = len(self.settlements_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} settlements with invalid dates")
        
        # Clean merchant names
        if 'merchant_display_name' in self.settlements_df.columns:
            before_count = len(self.settlements_df)
            self.settlements_df = self.settlements_df.dropna(subset=['merchant_display_name'])
            after_count = len(self.settlements_df)
            if before_count != after_count:
                logger.info(f"🗑️ Dropped {before_count - after_count} settlements with missing merchant names")
            
            self.settlements_df['merchant_name'] = self.settlements_df['merchant_display_name']
        
        final_count = len(self.settlements_df)
        logger.info(f"✅ Settlement data ready: {final_count} records (dropped {original_count - final_count})")
    
    def _clean_support_data(self):
        """Clean support data by dropping nulls"""
        if self.support_df.empty:
            return
        
        logger.info("🧹 Cleaning support data...")
        original_count = len(self.support_df)
        
        # Drop mostly empty columns
        null_threshold = 0.8
        null_percentages = self.support_df.isnull().sum() / len(self.support_df)
        columns_to_drop = null_percentages[null_percentages > null_threshold].index.tolist()
        
        if columns_to_drop:
            logger.info(f"🗑️ Dropping mostly empty support columns: {columns_to_drop}")
            self.support_df = self.support_df.drop(columns=columns_to_drop)
        
        final_count = len(self.support_df)
        logger.info(f"✅ Support data ready: {final_count} records (dropped {original_count - final_count})")
    
    def get_transactions(self, merchant_id: Optional[str] = None, days: int = 30) -> pd.DataFrame:
        """Get transaction data"""
        if self.transactions_df.empty:
            logger.warning("❌ No transaction data available")
            return pd.DataFrame()
        
        df = self.transactions_df.copy()
        logger.info(f"📊 Retrieved {len(df)} transactions")
        
        # Filter by date range
        if 'created_at' in df.columns:
            cutoff_date = datetime.now() - timedelta(days=days)
            original_count = len(df)
            df = df[df['created_at'] >= cutoff_date]
            logger.info(f"📅 Filtered to last {days} days: {len(df)} transactions (from {original_count})")
        
        return df
    
    def get_settlements(self, days: int = 30) -> pd.DataFrame:
        """Get settlement data"""
        if self.settlements_df.empty:
            logger.warning("❌ No settlement data available")
            return pd.DataFrame()
        
        df = self.settlements_df.copy()
        logger.info(f"📊 Retrieved {len(df)} settlements")
        
        # Filter by date range if settlement_date exists
        if 'settlement_date' in df.columns:
            cutoff_date = datetime.now() - timedelta(days=days)
            original_count = len(df)
            df = df[df['settlement_date'] >= cutoff_date]
            logger.info(f"📅 Filtered to last {days} days: {len(df)} settlements (from {original_count})")
        
        return df
    
    def get_merchant_names(self) -> List[str]:
        """Get list of unique merchant names"""
        if self.transactions_df.empty:
            logger.warning("❌ No transaction data available")
            return []
        
        if 'merchant_name' not in self.transactions_df.columns:
            logger.warning("❌ No merchant_name column found in transactions")
            return []
        
        # Since we already dropped nulls during cleaning, just get unique values
        merchant_names = self.transactions_df['merchant_name'].unique().tolist()
        logger.info(f"👥 Found {len(merchant_names)} unique merchants")
        
        return merchant_names
    
    def get_data_summary(self) -> Dict[str, Any]:
        """Get summary of loaded data"""
        summary = {
            'transactions_loaded': len(self.transactions_df) if not self.transactions_df.empty else 0,
            'settlements_loaded': len(self.settlements_df) if not self.settlements_df.empty else 0,
            'support_loaded': len(self.support_df) if not self.support_df.empty else 0,
        }
        
        if not self.transactions_df.empty:
            # All data is clean, so we can directly calculate without null checks
            summary.update({
                'date_range': {
                    'start': str(self.transactions_df['created_at'].min()) if 'created_at' in self.transactions_df.columns else None,
                    'end': str(self.transactions_df['created_at'].max()) if 'created_at' in self.transactions_df.columns else None
                },
                'merchants': self.transactions_df['merchant_name'].unique().tolist()[:10] if 'merchant_name' in self.transactions_df.columns else [],
                'total_amount': float(self.transactions_df['amount'].sum()) if 'amount' in self.transactions_df.columns else 0,
                'payment_methods': self.transactions_df['payment_method'].unique().tolist() if 'payment_method' in self.transactions_df.columns else [],
                'transaction_statuses': self.transactions_df['status'].unique().tolist() if 'status' in self.transactions_df.columns else [],
                'total_merchants': self.transactions_df['merchant_name'].nunique() if 'merchant_name' in self.transactions_df.columns else 0
            })
        
        return summary