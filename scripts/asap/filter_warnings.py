#!/usr/bin/env python3
"""
Script to filter the large warnings CSV file to target countries only using DuckDB.
This reduces the 3GB file to a manageable size for analysis.
DuckDB is much more efficient for large file operations and can query CSVs directly.
"""
import sys
import logging
import time
import duckdb

from src.asap.config import (
    WARNINGS_FILE, FILTERED_WARNINGS_FILE, TARGET_COUNTRIES,
    WARNINGS_SEPARATOR, WARNINGS_COUNTRY_COL
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def filter_warnings_duckdb():
    """Filter the warnings CSV file using DuckDB for optimal performance."""
    logger.info(f"Starting DuckDB filtering of {WARNINGS_FILE}")
    logger.info(f"Target countries: {TARGET_COUNTRIES}")
    logger.info(f"Output file: {FILTERED_WARNINGS_FILE}")
    
    # Ensure output directory exists
    FILTERED_WARNINGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    # Get original file size for comparison
    original_size = WARNINGS_FILE.stat().st_size / (1024**3)  # GB
    logger.info(f"Original file size: {original_size:.2f} GB")
    
    # Create DuckDB connection
    conn = duckdb.connect()
    
    # Create the country filter as SQL IN clause
    country_list = "', '".join(TARGET_COUNTRIES)
    country_filter = f"'{country_list}'"
    
    # Build SQL query to filter and export
    # Note: DuckDB can read CSV files directly and handles the semicolon separator
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
        
        logger.info(f"Filtering complete in {elapsed_time:.2f} seconds!")
        logger.info(f"Total rows filtered: {total_filtered_rows:,}")
        
        # Check file size reduction
        filtered_size = FILTERED_WARNINGS_FILE.stat().st_size / (1024**3)  # GB
        reduction = (1 - filtered_size/original_size) * 100 if original_size > 0 else 0
        
        logger.info(f"Filtered file size: {filtered_size:.3f} GB")
        logger.info(f"Size reduction: {reduction:.1f}%")
        
        # Display performance comparison
        estimated_pandas_time = original_size * 60  # Rough estimate
        speedup = estimated_pandas_time / elapsed_time if elapsed_time > 0 else 1
        logger.info(f"DuckDB speedup vs pandas (estimated): {speedup:.1f}x faster")
        
    except Exception as e:
        logger.error(f"Error during DuckDB filtering: {e}")
        raise
    finally:
        conn.close()


def main():
    """Main function."""
    try:
        filter_warnings_duckdb()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during filtering: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()