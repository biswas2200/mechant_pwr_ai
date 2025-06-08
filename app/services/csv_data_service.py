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
        """Clean transaction data"""
        if self.transactions_df.empty:
            return
        
        logger.info("🧹 Cleaning transaction data...")
        logger.info(f"📋 Original columns: {list(self.transactions_df.columns)}")
        
        # Show first few rows for debugging
        logger.info(f"📝 Sample data (first 2 rows):")
        for i, row in self.transactions_df.head(2).iterrows():
            logger.info(f"   Row {i}: {dict(row)}")
        
        # Updated column mapping based on actual txn_refunds.csv structure
        column_mapping = {
            # Direct mappings for txn_refunds.csv
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
            
            # Fallback mappings
            'transaction_amount': 'amount',
            'payment_method': 'payment_method',
            'transaction_status': 'status',
            'transaction_date': 'created_at',
            'merchant_name': 'merchant_name'
        }
        
        # Apply mapping
        for old_col, new_col in column_mapping.items():
            if old_col in self.transactions_df.columns and new_col not in self.transactions_df.columns:
                self.transactions_df[new_col] = self.transactions_df[old_col]
                logger.info(f"✅ Mapped {old_col} → {new_col}")
        
        # Clean amount - handle both regular amount and convenience fees
        if 'amount' in self.transactions_df.columns:
            logger.info("💰 Processing amounts...")
            self.transactions_df['amount'] = pd.to_numeric(self.transactions_df['amount'], errors='coerce')
            
            # Check if amounts are in paise (convenience fees columns suggest paise usage)
            sample_amounts = self.transactions_df['amount'].dropna().head(10)
            median_amount = self.transactions_df['amount'].median()
            
            # Handle case where all amounts are NaN
            if pd.isna(median_amount):
                logger.warning("💰 All amounts are invalid, setting to 0")
                median_amount = 0
            
            logger.info(f"💰 Sample amounts: {list(sample_amounts)}")
            logger.info(f"💰 Median amount: {median_amount}")
            
            # Check if convenience fees are in paise
            has_convenience_paise = 'convenience_fees_amt_in_paise' in self.transactions_df.columns
            if has_convenience_paise and median_amount > 10000:  # Likely main amount is also in paise
                logger.info("💰 Converting amounts from paise to rupees")
                self.transactions_df['amount'] = self.transactions_df['amount'] / 100
            
            self.transactions_df.dropna(inplace=True)
            # Fill NaN values with 0 and ensure no inf values
            self.transactions_df['amount'] = self.transactions_df['amount'].fillna(0)
            self.transactions_df['amount'] = self.transactions_df['amount'].replace([float('inf'), float('-inf')], 0)
            
            logger.info(f"💰 Final amount range: ₹{self.transactions_df['amount'].min():.2f} to ₹{self.transactions_df['amount'].max():.2f}")
        
        # Clean datetime - prioritize transaction_start_date_time
        date_columns = ['transaction_start_date_time', 'created_at', 'sale_txn_date_time', 'txn_completion_date_time']
        date_found = False
        
        for date_col in date_columns:
            if date_col in self.transactions_df.columns:
                logger.info(f"📅 Processing date column: {date_col}")
                try:
                    self.transactions_df['created_at'] = pd.to_datetime(self.transactions_df[date_col], errors='coerce')
                    self.transactions_df['date'] = self.transactions_df['created_at'].dt.date.astype(str)
                    
                    valid_dates = self.transactions_df['created_at'].notna().sum()
                    logger.info(f"📅 Valid dates: {valid_dates} out of {len(self.transactions_df)}")
                    
                    if valid_dates > 0:
                        logger.info(f"📅 Date range: {self.transactions_df['created_at'].min()} to {self.transactions_df['created_at'].max()}")
                        date_found = True
                        break
                except Exception as e:
                    logger.warning(f"📅 Error processing {date_col}: {e}")
                    continue
        
        if not date_found:
            logger.info("📅 No valid date column found, using current date")
            self.transactions_df['created_at'] = datetime.now()
            self.transactions_df['date'] = datetime.now().date().isoformat()
        
        # Ensure created_at has no NaT values
        if 'created_at' in self.transactions_df.columns:
            nat_count = self.transactions_df['created_at'].isna().sum()
            if nat_count > 0:
                logger.info(f"📅 Filling {nat_count} NaT dates with current time")
                self.transactions_df['created_at'].fillna(datetime.now(), inplace=True)
                self.transactions_df['date'] = self.transactions_df['created_at'].dt.date.astype(str)
        
        # Clean status using txn_status_name
        if 'txn_status_name' in self.transactions_df.columns:
            self.transactions_df['status'] = self.transactions_df['txn_status_name']
        
        if 'status' in self.transactions_df.columns:
            logger.info("✅ Processing transaction statuses...")
            
            # Show original status values
            original_statuses = self.transactions_df['status'].value_counts().head(10)
            logger.info(f"✅ Original statuses: {original_statuses.to_dict()}")
            
            status_mapping = {
                'SUCCESS': 'SUCCESS', 'SUCCESSFUL': 'SUCCESS', 'CAPTURED': 'SUCCESS',
                'SETTLED': 'SUCCESS', 'COMPLETED': 'SUCCESS', 'APPROVED': 'SUCCESS',
                'FAILED': 'FAILED', 'FAILURE': 'FAILED', 'DECLINED': 'FAILED',
                'TIMEOUT': 'FAILED', 'ERROR': 'FAILED', 'REJECTED': 'FAILED',
                'PENDING': 'PENDING', 'INITIATED': 'PENDING', 'PROCESSING': 'PENDING'
            }
            
            self.transactions_df['status'] = (
                self.transactions_df['status'].astype(str).str.upper()
                .map(status_mapping).fillna('SUCCESS')  # Default to SUCCESS for unknown
            )
            
            final_statuses = self.transactions_df['status'].value_counts()
            logger.info(f"✅ Final status distribution: {final_statuses.to_dict()}")
        else:
            logger.info("✅ No status column found, setting all to SUCCESS")
            self.transactions_df['status'] = 'SUCCESS'
        
        # Clean payment methods using payment_mode_name
        if 'payment_mode_name' in self.transactions_df.columns:
            self.transactions_df['payment_method'] = self.transactions_df['payment_mode_name']
        
        if 'payment_method' in self.transactions_df.columns:
            logger.info("💳 Processing payment methods...")
            
            original_methods = self.transactions_df['payment_method'].value_counts().head(10)
            logger.info(f"💳 Original payment methods: {original_methods.to_dict()}")
            
            payment_mapping = {
                'CREDIT CARD': 'CREDIT_CARD', 'DEBIT CARD': 'DEBIT_CARD',
                'UPI': 'UPI', 'NET BANKING': 'NET_BANKING',
                'WALLET': 'WALLET', 'EMI': 'EMI',
                'CREDIT_CARD': 'CREDIT_CARD', 'DEBIT_CARD': 'DEBIT_CARD'
            }
            
            self.transactions_df['payment_method'] = (
                self.transactions_df['payment_method'].astype(str).str.upper()
                .map(payment_mapping).fillna(self.transactions_df['payment_method'].astype(str).str.upper())
            )
            
            final_methods = self.transactions_df['payment_method'].value_counts()
            logger.info(f"💳 Final payment methods: {final_methods.to_dict()}")
        else:
            logger.info("💳 No payment method column found, setting all to UPI")
            self.transactions_df['payment_method'] = 'UPI'
        
        # Set merchant info using merchant_display_name
        if 'merchant_display_name' in self.transactions_df.columns:
            self.transactions_df['merchant_name'] = self.transactions_df['merchant_display_name']
        elif 'merchant_name' not in self.transactions_df.columns:
            self.transactions_df['merchant_name'] = 'CSV_MERCHANT'
        
        self.transactions_df['merchant_id'] = 'CSV_MERCHANT_001'
        
        # Show final summary
        unique_merchants = self.transactions_df['merchant_name'].nunique()
        logger.info(f"👥 Unique merchants: {unique_merchants}")
        
        if unique_merchants <= 5:
            merchant_names = list(self.transactions_df['merchant_name'].unique())
            logger.info(f"👥 Merchant names: {merchant_names}")
        
        logger.info(f"🎉 Transaction data cleaning complete!")
        logger.info(f"📊 Final dataset: {len(self.transactions_df)} transactions ready")
    
    def _clean_settlement_data(self):
        """Clean settlement data"""
        if self.settlements_df.empty:
            return
        
        logger.info("🧹 Cleaning settlement data...")
        
        # Clean settlement amounts
        amount_columns = ['amount', 'settlement_amount', 'actual_txn_amount', 'refund_amount']
        for col in amount_columns:
            if col in self.settlements_df.columns:
                self.settlements_df[col] = pd.to_numeric(self.settlements_df[col], errors='coerce').fillna(0)
                
                # Convert from paise if needed
                median_val = self.settlements_df[col].median()
                if pd.notna(median_val) and median_val > 10000:  # Likely in paise
                    logger.info(f"💰 Converting {col} from paise to rupees")
                    self.settlements_df[col] = self.settlements_df[col] / 100
                
                # Ensure no inf values
                self.settlements_df[col] = self.settlements_df[col].replace([float('inf'), float('-inf')], 0)
        
        self.settlements_df.dropna(inplace=True)
        # Clean settlement dates
        if 'transaction_start_date_time' in self.settlements_df.columns:
            self.settlements_df['settlement_date'] = pd.to_datetime(
                self.settlements_df['transaction_start_date_time'], errors='coerce'
            )
            # Fill NaT values with current time
            self.settlements_df['settlement_date'].fillna(datetime.now(), inplace=True)
        
        # Clean merchant names
        if 'merchant_display_name' in self.settlements_df.columns:
            self.settlements_df['merchant_name'] = self.settlements_df['merchant_display_name'].fillna('UNKNOWN_MERCHANT')
        
        logger.info(f"✅ Settlement data ready: {len(self.settlements_df)} records")
    
    def _clean_support_data(self):
        """Clean support data"""
        if self.support_df.empty:
            return
        
        logger.info("🧹 Cleaning support data...")
        # Basic support cleaning - can be expanded based on actual support data structure
        logger.info(f"✅ Support data ready: {len(self.support_df)} records")
    
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
        
        # Clean NaN values before returning
        df = self._clean_dataframe_for_json(df)
        
        return df
    
    def _clean_dataframe_for_json(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame to remove NaN values that cause JSON serialization issues"""
        if df.empty:
            return df
        
        # Replace NaN values based on column types
        for col in df.columns:
            if df[col].dtype in ['float64', 'float32']:
                # Replace NaN in numeric columns with 0
                df[col] = df[col].fillna(0)
                # Replace inf values with 0
                df[col] = df[col].replace([float('inf'), float('-inf')], 0)
            elif df[col].dtype == 'object':
                # Replace NaN in string/object columns with empty string
                df[col] = df[col].fillna('')
            elif 'datetime' in str(df[col].dtype):
                # Replace NaT in datetime columns with a default date
                df[col] = df[col].fillna(datetime.now())
        
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
        
        # Clean NaN values before returning
        df = self._clean_dataframe_for_json(df)
        
        return df
    
    def get_merchant_names(self) -> List[str]:
        """Get list of unique merchant names"""
        if self.transactions_df.empty:
            logger.warning("❌ No transaction data available")
            return []
        
        if 'merchant_name' not in self.transactions_df.columns:
            logger.warning("❌ No merchant_name column found in transactions")
            return []
        
        # Get unique merchant names, excluding NaN values, and convert to strings
        merchant_names = (
            self.transactions_df['merchant_name']
            .dropna()  # Remove NaN values
            .astype(str)  # Convert to string to handle any remaining edge cases
            .unique()
            .tolist()
        )
        
        # Filter out empty strings and 'nan' strings
        merchant_names = [name for name in merchant_names if name and name.lower() != 'nan']
        
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
            # Calculate total amount safely
            total_amount = 0
            if 'amount' in self.transactions_df.columns:
                amount_sum = self.transactions_df['amount'].sum()
                total_amount = float(amount_sum) if not pd.isna(amount_sum) else 0
            
            # Get date range safely
            date_start = None
            date_end = None
            if 'created_at' in self.transactions_df.columns:
                min_date = self.transactions_df['created_at'].min()
                max_date = self.transactions_df['created_at'].max()
                date_start = str(min_date) if not pd.isna(min_date) else None
                date_end = str(max_date) if not pd.isna(max_date) else None
            
            # Get merchants safely
            merchants = []
            total_merchants = 0
            if 'merchant_name' in self.transactions_df.columns:
                unique_merchants = self.transactions_df['merchant_name'].dropna().unique()
                merchants = [str(m) for m in unique_merchants[:10]]  # Convert to string to avoid NaN issues
                total_merchants = len(unique_merchants)
            
            # Get payment methods safely
            payment_methods = []
            if 'payment_method' in self.transactions_df.columns:
                unique_methods = self.transactions_df['payment_method'].dropna().unique()
                payment_methods = [str(m) for m in unique_methods]
            
            # Get transaction statuses safely
            transaction_statuses = []
            if 'status' in self.transactions_df.columns:
                unique_statuses = self.transactions_df['status'].dropna().unique()
                transaction_statuses = [str(s) for s in unique_statuses]
            
            summary.update({
                'date_range': {
                    'start': date_start,
                    'end': date_end
                },
                'merchants': merchants,
                'total_amount': total_amount,
                'payment_methods': payment_methods,
                'transaction_statuses': transaction_statuses,
                'total_merchants': total_merchants
            })
        
        return summary