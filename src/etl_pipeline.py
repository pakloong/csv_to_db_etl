"""
Data Engineer: ETL Pipeline Implementation
CSV to Database ETL Pipeline with Validation and Error Handling
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, inspect
from datetime import datetime
import logging
import os
from pathlib import Path

# ============================================================================
# 1. LOGGING CONFIGURATION
# ============================================================================

def setup_logger(log_dir='logs', log_filename=None):
    """Setup logging configuration"""
    
    # Create logs directory if not exists
    Path(log_dir).mkdir(exist_ok=True)
    
    # Generate log filename with timestamp
    if log_filename is None:
        log_filename = f"etl_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.log"
    
    log_filepath = os.path.join(log_dir, log_filename)
    
    # Create logger
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    
    # File handler
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

# ============================================================================
# 2. CONFIGURATION
# ============================================================================

class ETLConfig:
    """ETL Configuration"""
    
    # Database Configuration
    DB_HOST = 'localhost'
    DB_PORT = 5432
    DB_NAME = 'data_warehouse'
    DB_USER = 'postgres'
    DB_PASSWORD = 'password'
    
    # Path Configuration
    INPUT_DIR = './data/input/'
    OUTPUT_DIR = './data/output/'
    ARCHIVE_DIR = './data/archive/'
    
    # Processing Configuration
    CHUNK_SIZE = 1000
    MAX_RETRIES = 3
    ON_ERROR_ACTION = 'log_and_continue'  # or 'halt'
    
    @property
    def connection_string(self):
        """Generate PostgreSQL connection string"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

# ============================================================================
# 3. EXTRACT STAGE
# ============================================================================

class DataExtractor:
    """Extract data from source files"""
    
    def __init__(self, config):
        self.config = config
        logger.info("DataExtractor initialized")
    
    def extract_csv(self, filepath):
        """
        Extract data from CSV file
        
        Args:
            filepath (str): Path to CSV file
            
        Returns:
            pd.DataFrame: Extracted data
        """
        try:
            logger.info(f"Extracting data from: {filepath}")
            
            # Check file exists
            if not os.path.exists(filepath):
                raise FileNotFoundError(f"File not found: {filepath}")
            
            # Read CSV
            df = pd.read_csv(filepath)
            
            logger.info(f"✓ Successfully extracted {len(df)} rows from {filepath}")
            logger.debug(f"  Columns: {list(df.columns)}")
            logger.debug(f"  Data types: {dict(df.dtypes)}")
            
            return df
            
        except FileNotFoundError as e:
            logger.error(f"File not found: {str(e)}")
            raise
        except pd.errors.ParserError as e:
            logger.error(f"CSV parsing error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error extracting CSV: {str(e)}")
            raise
    
    def extract_from_directory(self, directory, file_pattern='*.csv'):
        """Extract multiple CSV files from directory"""
        
        csv_files = list(Path(directory).glob(file_pattern))
        logger.info(f"Found {len(csv_files)} CSV files in {directory}")
        
        all_data = []
        for csv_file in csv_files:
            try:
                df = self.extract_csv(str(csv_file))
                all_data.append(df)
            except Exception as e:
                logger.warning(f"Skipping file {csv_file}: {str(e)}")
                continue
        
        # Combine all dataframes
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"✓ Combined {len(csv_files)} files: {len(combined_df)} total rows")
            return combined_df
        else:
            logger.error("No valid CSV files found")
            raise ValueError("No valid CSV files found")

# ============================================================================
# 4. TRANSFORM STAGE
# ============================================================================

class DataTransformer:
    """Transform and validate data"""
    
    def __init__(self, config):
        self.config = config
        self.validation_errors = []
        logger.info("DataTransformer initialized")
    
    def validate_schema(self, df, expected_columns):
        """
        Validate that DataFrame has expected columns
        
        Args:
            df (pd.DataFrame): Data to validate
            expected_columns (list): Expected column names
            
        Returns:
            bool: True if valid, False otherwise
        """
        missing_cols = set(expected_columns) - set(df.columns)
        extra_cols = set(df.columns) - set(expected_columns)
        
        if missing_cols:
            error = f"Missing columns: {missing_cols}"
            logger.error(error)
            self.validation_errors.append(error)
            return False
        
        if extra_cols:
            logger.warning(f"Extra columns found: {extra_cols}")
        
        logger.info("✓ Schema validation passed")
        return True
    
    def remove_duplicates(self, df, subset=None):
        """Remove duplicate rows"""
        initial_count = len(df)
        df = df.drop_duplicates(subset=subset)
        removed_count = initial_count - len(df)
        
        if removed_count > 0:
            logger.info(f"✓ Removed {removed_count} duplicate rows")
        
        return df
    
    def handle_missing_values(self, df):
        """Handle missing values"""
        logger.info("Handling missing values...")
        
        missing_before = df.isnull().sum().sum()
        
        # For numeric columns, fill with median
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if df[col].isnull().sum() > 0:
                median_val = df[col].median()
                df[col].fillna(median_val, inplace=True)
                logger.debug(f"  Filled {col} with median: {median_val:.2f}")
        
        # For categorical columns, fill with 'Unknown'
        categorical_cols = df.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            if df[col].isnull().sum() > 0:
                df[col].fillna('Unknown', inplace=True)
                logger.debug(f"  Filled {col} with 'Unknown'")
        
        missing_after = df.isnull().sum().sum()
        logger.info(f"✓ Missing values: {missing_before} → {missing_after}")
        
        return df
    
    def validate_data_types(self, df, type_mappings):
        """
        Validate and convert data types
        
        Args:
            df (pd.DataFrame): Data to validate
            type_mappings (dict): Column name to expected type
        """
        logger.info("Validating data types...")
        
        for col, expected_type in type_mappings.items():
            if col not in df.columns:
                logger.warning(f"Column not found: {col}")
                continue
            
            try:
                if expected_type == 'datetime':
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                elif expected_type == 'numeric':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                elif expected_type == 'string':
                    df[col] = df[col].astype(str)
                
                logger.debug(f"  Converted {col} to {expected_type}")
            except Exception as e:
                error = f"Error converting {col} to {expected_type}: {str(e)}"
                logger.warning(error)
                self.validation_errors.append(error)
        
        logger.info("✓ Data type validation complete")
        return df
    
    def validate_value_ranges(self, df, value_constraints):
        """
        Validate value ranges
        
        Args:
            df (pd.DataFrame): Data to validate
            value_constraints (dict): Column name to (min, max) tuple
        """
        logger.info("Validating value ranges...")
        
        invalid_rows = 0
        for col, (min_val, max_val) in value_constraints.items():
            if col not in df.columns:
                logger.warning(f"Column not found: {col}")
                continue
            
            # Remove rows outside valid range
            before_count = len(df)
            df = df[(df[col] >= min_val) & (df[col] <= max_val)]
            after_count = len(df)
            
            removed = before_count - after_count
            if removed > 0:
                invalid_rows += removed
                logger.warning(f"  Removed {removed} rows with {col} outside [{min_val}, {max_val}]")
        
        if invalid_rows > 0:
            logger.info(f"✓ Removed {invalid_rows} rows with invalid values")
        
        return df
    
    def transform(self, df, schema_check=None, type_mappings=None, value_constraints=None):
        """
        Complete transformation pipeline
        
        Args:
            df (pd.DataFrame): Raw data
            schema_check (list): Expected columns
            type_mappings (dict): Data type conversions
            value_constraints (dict): Valid value ranges
            
        Returns:
            pd.DataFrame: Transformed data
        """
        logger.info("="*60)
        logger.info("TRANSFORMATION STAGE")
        logger.info("="*60)
        
        df_clean = df.copy()
        
        # Schema validation
        if schema_check and not self.validate_schema(df_clean, schema_check):
            raise ValueError("Schema validation failed")
        
        # Remove duplicates
        df_clean = self.remove_duplicates(df_clean)
        
        # Handle missing values
        df_clean = self.handle_missing_values(df_clean)
        
        # Validate and convert data types
        if type_mappings:
            df_clean = self.validate_data_types(df_clean, type_mappings)
        
        # Validate value ranges
        if value_constraints:
            df_clean = self.validate_value_ranges(df_clean, value_constraints)
        
        logger.info(f"✓ Transformation complete: {len(df_clean)} rows")
        
        return df_clean

# ============================================================================
# 5. LOAD STAGE
# ============================================================================

class DataLoader:
    """Load data to database"""
    
    def __init__(self, config):
        self.config = config
        logger.info("DataLoader initialized")
    
    def create_connection(self):
        """Create database connection"""
        try:
            engine = create_engine(self.config.connection_string)
            logger.info(f"✓ Connected to database: {self.config.DB_NAME}")
            return engine
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            raise
    
    def check_table_exists(self, engine, table_name):
        """Check if table exists"""
        inspector = inspect(engine)
        exists = inspector.has_table(table_name)
        logger.info(f"Table '{table_name}' exists: {exists}")
        return exists
    
    def load_data(self, df, table_name, if_exists='append', chunk_size=1000):
        """
        Load data to PostgreSQL
        
        Args:
            df (pd.DataFrame): Data to load
            table_name (str): Target table name
            if_exists (str): 'fail', 'replace', or 'append'
            chunk_size (int): Rows per insert
        """
        try:
            logger.info("="*60)
            logger.info("LOAD STAGE")
            logger.info("="*60)
            
            engine = self.create_connection()
            
            logger.info(f"Loading {len(df)} rows to table '{table_name}'...")
            logger.info(f"  If exists: {if_exists}")
            logger.info(f"  Chunk size: {chunk_size}")
            
            # Load data
            df.to_sql(
                table_name,
                engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size,
                method='multi'
            )
            
            logger.info(f"✓ Successfully loaded {len(df)} rows to '{table_name}'")
            
            # Verify load
            result = engine.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
            logger.info(f"  Verification: {row_count} rows in table")
            
            engine.dispose()
            
        except Exception as e:
            logger.error(f"Error loading data: {str(e)}")
            raise

# ============================================================================
# 6. ETL ORCHESTRATION
# ============================================================================

class ETLPipeline:
    """Main ETL Pipeline Orchestration"""
    
    def __init__(self, config):
        self.config = config
        self.extractor = DataExtractor(config)
        self.transformer = DataTransformer(config)
        self.loader = DataLoader(config)
        self.start_time = None
        self.end_time = None
    
    def run(self, input_file, table_name, schema_check=None, 
            type_mappings=None, value_constraints=None):
        """
        Run complete ETL pipeline
        
        Args:
            input_file (str): Source CSV file
            table_name (str): Target table name
            schema_check (list): Expected columns
            type_mappings (dict): Data type conversions
            value_constraints (dict): Valid value ranges
        """
        
        self.start_time = datetime.now()
        
        logger.info("\n" + "="*60)
        logger.info("ETL PIPELINE STARTED")
        logger.info("="*60)
        logger.info(f"Start time: {self.start_time}")
        
        try:
            # EXTRACT
            logger.info("\n[EXTRACT] Loading data from source...")
            df_raw = self.extractor.extract_csv(input_file)
            
            # TRANSFORM
            logger.info("\n[TRANSFORM] Cleaning and validating data...")
            df_clean = self.transformer.transform(
                df_raw,
                schema_check=schema_check,
                type_mappings=type_mappings,
                value_constraints=value_constraints
            )
            
            # LOAD
            logger.info("\n[LOAD] Loading data to database...")
            self.loader.load_data(df_clean, table_name, if_exists='append')
            
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            logger.info("\n" + "="*60)
            logger.info("✓ ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            logger.info(f"End time: {self.end_time}")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Rows processed: {len(df_clean):,}")
            logger.info(f"Throughput: {len(df_clean)/duration:,.0f} rows/second")
            
            return df_clean
            
        except Exception as e:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            logger.error("\n" + "="*60)
            logger.error("✗ ETL PIPELINE FAILED")
            logger.error("="*60)
            logger.error(f"Error: {str(e)}")
            logger.error(f"Duration: {duration:.2f} seconds")
            
            raise

# ============================================================================
# 7. UTILITY FUNCTIONS
# ============================================================================

def archive_file(filepath, archive_dir):
    """Archive processed file"""
    try:
        filename = os.path.basename(filepath)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_filename = f"{timestamp}_{filename}"
        archive_path = os.path.join(archive_dir, archive_filename)
        
        # Create archive directory if not exists
        Path(archive_dir).mkdir(parents=True, exist_ok=True)
        
        # Move file
        os.rename(filepath, archive_path)
        logger.info(f"✓ Archived file: {archive_path}")
        
    except Exception as e:
        logger.warning(f"Could not archive file: {str(e)}")

# ============================================================================
# 8. MAIN EXECUTION
# ============================================================================

if __name__ == "__main__":
    
    # Initialize configuration
    config = ETLConfig()
    
    # Define schema and validation rules
    EXPECTED_COLUMNS = ['order_id', 'customer_id', 'order_date', 'product_id', 'price']
    
    TYPE_MAPPINGS = {
        'order_date': 'datetime',
        'price': 'numeric',
        'customer_id': 'string',
        'product_id': 'string'
    }
    
    VALUE_CONSTRAINTS = {
        'price': (0, 100000)  # Price between 0 and 100,000
    }
    
    # Run ETL Pipeline
    pipeline = ETLPipeline(config)
    
    try:
        df = pipeline.run(
            input_file='./data/input/sales_transactions.csv',
            table_name='sales_transactions',
            schema_check=EXPECTED_COLUMNS,
            type_mappings=TYPE_MAPPINGS,
            value_constraints=VALUE_CONSTRAINTS
        )
        
        # Archive source file
        archive_file('./data/input/sales_transactions.csv', config.ARCHIVE_DIR)
        
        logger.info("\n✓ All operations completed successfully!")
        
    except Exception as e:
        logger.critical(f"Pipeline execution failed: {str(e)}")
        raise
