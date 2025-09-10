#!/usr/bin/env python3
"""
Script to filter the large warnings CSV file to target countries only using DuckDB.
This reduces the 3GB file to a manageable size for analysis.
Supports both local files and Azure Blob Storage.
"""
import sys
import logging
import time
import argparse

from src.asap.config import (
    WARNINGS_FILE, FILTERED_WARNINGS_FILE, TARGET_COUNTRIES,
    WARNINGS_SEPARATOR, WARNINGS_COUNTRY_COL
)
from src.asap.azure_config import (
    USE_BLOB_STORAGE, validate_azure_config,
    get_warnings_url, get_filtered_warnings_url, log_azure_info
)
from src.asap.blob_utils import (
    get_azure_connection, copy_blob_to_blob, get_blob_info
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def filter_warnings_local():
    """Filter warnings using local DuckDB for local files."""
    logger.info(f"Starting local DuckDB filtering of {WARNINGS_FILE}")
    logger.info(f"Target countries: {TARGET_COUNTRIES}")
    logger.info(f"Output file: {FILTERED_WARNINGS_FILE}")
    
    # Ensure output directory exists
    FILTERED_WARNINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Get original file size for comparison
    original_size = WARNINGS_FILE.stat().st_size / (1024**3)  # GB
    logger.info(f"Original file size: {original_size:.2f} GB")
    
    # Create DuckDB connection
    import duckdb
    conn = duckdb.connect()
    
    # Create the country filter as SQL IN clause
    country_list = "', '".join(TARGET_COUNTRIES)
    country_filter = f"'{country_list}'"
    
    # Build SQL query to filter and export
    sql_query = f"""
    COPY (
        SELECT * 
        FROM read_csv_auto('{WARNINGS_FILE}', delim='{WARNINGS_SEPARATOR}')
        WHERE TRIM(TRIM({WARNINGS_COUNTRY_COL}, '"')) IN ({country_filter})
    ) TO '{FILTERED_WARNINGS_FILE}' (DELIMITER '{WARNINGS_SEPARATOR}', HEADER);
    """
    
    logger.info("Executing DuckDB query...")
    start_time = time.time()
    
    try:
        # Execute the filtering query
        conn.execute(sql_query)
        
        # Get count of filtered rows for reporting
        count_query = f"""
        SELECT COUNT(*) as total_rows
        FROM read_csv_auto('{FILTERED_WARNINGS_FILE}', delim='{WARNINGS_SEPARATOR}')
        """
        result = conn.execute(count_query).fetchone()
        total_filtered_rows = result[0] if result else 0
        
        elapsed_time = time.time() - start_time
        
        logger.info(f"Local filtering complete in {elapsed_time:.2f} seconds!")
        logger.info(f"Total rows filtered: {total_filtered_rows:,}")
        
        # Check file size reduction
        filtered_size = FILTERED_WARNINGS_FILE.stat().st_size / (1024**3)  # GB
        reduction = (1 - filtered_size/original_size) * 100 if original_size > 0 else 0
        
        logger.info(f"Filtered file size: {filtered_size:.3f} GB")
        logger.info(f"Size reduction: {reduction:.1f}%")
        
    except Exception as e:
        logger.error(f"Error during local filtering: {e}")
        raise
    finally:
        conn.close()


def filter_warnings_blob():
    """Filter warnings using DuckDB with Azure Blob Storage."""
    if not validate_azure_config():
        raise ValueError("Invalid Azure configuration")
    
    logger.info("Starting Azure Blob filtering with DuckDB")
    log_azure_info()
    
    # Create country filter WHERE clause
    country_list = "', '".join(TARGET_COUNTRIES)
    where_clause = f"TRIM(TRIM({WARNINGS_COUNTRY_COL}, '\"')) IN ('{country_list}')"
    
    try:
        # Check local warnings file exists
        from src.asap.config import WARNINGS_FILE
        if not WARNINGS_FILE.exists():
            raise FileNotFoundError(f"Local warnings file not found: {WARNINGS_FILE}")
        
        logger.info(f"Processing local warnings file: {WARNINGS_FILE}")
        start_time = time.time()
        
        # Step 1: Filter locally using DuckDB (fast)
        import tempfile
        from src.asap.config import FILTERED_WARNINGS_FILE
        local_warnings_path = str(WARNINGS_FILE)
        temp_filtered_path = str(FILTERED_WARNINGS_FILE)
        
        # Ensure output directory exists
        FILTERED_WARNINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Filter locally first
        import duckdb
        conn = duckdb.connect()
        filter_query = f"""
        COPY (
            SELECT * FROM read_csv_auto('{local_warnings_path}', delim=';')
            WHERE {where_clause}
        ) TO '{temp_filtered_path}' (DELIMITER ';', HEADER)
        """
        
        logger.info("Filtering data locally with DuckDB...")
        conn.execute(filter_query)
        conn.close()
        
        # Step 2: Upload filtered file to blob storage
        logger.info("Uploading filtered data to blob storage...")
        
        # Use Azure Blob SDK to upload the filtered CSV
        from azure.storage.blob import BlobServiceClient
        from src.asap.azure_config import STORAGE_ACCOUNT, SAS_TOKEN, CONTAINER
        
        # Create blob service client
        account_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
        blob_service_client = BlobServiceClient(account_url=account_url, credential=SAS_TOKEN)
        
        # Upload file to blob
        blob_path = "ds-rosea-thresholds/processed/asap/warnings_filtered.csv"
        
        with open(temp_filtered_path, 'rb') as data:
            blob_client = blob_service_client.get_blob_client(
                container=CONTAINER, 
                blob=blob_path
            )
            blob_client.upload_blob(data, overwrite=True)
        
        logger.info(f"Successfully uploaded filtered data to blob: {blob_path}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Blob filtering complete in {elapsed_time:.2f} seconds!")
        
        # Get filtered blob info
        filtered_info = get_blob_info("ds-rosea-thresholds/processed/asap/warnings_filtered.csv")
        if filtered_info.get('exists'):
            filtered_rows = filtered_info.get('row_count', 0)
            logger.info(f"Filtered blob created with {filtered_rows:,} rows")
            
            if warnings_info.get('row_count'):
                original_rows = warnings_info.get('row_count')
                reduction = (1 - filtered_rows/original_rows) * 100
                logger.info(f"Row reduction: {reduction:.1f}%")
        
    except Exception as e:
        logger.error(f"Error during blob filtering: {e}")
        raise


def filter_warnings_duckdb():
    """Main filtering function - automatically chooses local or blob storage."""
    if USE_BLOB_STORAGE:
        filter_warnings_blob()
    else:
        filter_warnings_local()


def main():
    """Main function with command line argument support."""
    parser = argparse.ArgumentParser(description='Filter ASAP warnings data')
    parser.add_argument('--force-local', action='store_true', 
                       help='Force local processing even if blob storage is configured')
    parser.add_argument('--force-blob', action='store_true',
                       help='Force blob processing (requires Azure configuration)')
    parser.add_argument('--test-connection', action='store_true',
                       help='Test Azure blob connection and exit')
    
    args = parser.parse_args()
    
    # Test connection if requested
    if args.test_connection:
        from src.asap.blob_utils import test_blob_connection
        if test_blob_connection():
            logger.info("✅ Azure blob connection test successful")
            sys.exit(0)
        else:
            logger.error("❌ Azure blob connection test failed")
            sys.exit(1)
    
    try:
        # Determine processing mode
        if args.force_local:
            logger.info("Forcing local processing mode")
            filter_warnings_local()
        elif args.force_blob:
            logger.info("Forcing blob processing mode")
            filter_warnings_blob()
        else:
            # Use automatic detection
            logger.info(f"Auto-detected mode: {'blob' if USE_BLOB_STORAGE else 'local'}")
            filter_warnings_duckdb()
            
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during filtering: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()