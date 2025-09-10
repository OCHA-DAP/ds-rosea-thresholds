"""
Azure Blob Storage utilities for DuckDB operations.
Handles DuckDB connections and blob storage operations.
"""
import duckdb
import logging
import time
from typing import Optional, List
from pathlib import Path

from .azure_config import (
    USE_BLOB_STORAGE, SAS_TOKEN, STORAGE_ACCOUNT, validate_azure_config,
    get_blob_url, log_azure_info
)

logger = logging.getLogger(__name__)


class DuckDBBlobConnection:
    """Manager for DuckDB connections with Azure Blob Storage support."""
    
    def __init__(self):
        self.conn: Optional[duckdb.DuckDBPyConnection] = None
        self._initialized = False
    
    def __enter__(self):
        """Context manager entry."""
        return self.get_connection()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Get configured DuckDB connection with Azure support if needed.
        
        Returns:
            Configured DuckDB connection
        """
        if self.conn is None:
            self.conn = duckdb.connect()
            
        if not self._initialized and USE_BLOB_STORAGE:
            self._setup_azure()
            
        return self.conn
    
    def _setup_azure(self):
        """Set up Azure Blob Storage authentication for DuckDB."""
        if not validate_azure_config():
            raise ValueError("Invalid Azure configuration")
            
        logger.info("Setting up DuckDB with Azure Blob Storage...")
        
        try:
            # Install and load Azure extension
            self.conn.execute("INSTALL azure;")
            self.conn.execute("LOAD azure;")
            
            # Set Azure storage connection string for authentication  
            connection_string = f"DefaultEndpointsProtocol=https;AccountName={STORAGE_ACCOUNT};SharedAccessSignature={SAS_TOKEN}"
            self.conn.execute(f"SET azure_storage_connection_string = '{connection_string}';")
            
            self._initialized = True
            logger.info("Azure Blob Storage configured successfully")
            log_azure_info()
            
        except Exception as e:
            logger.error(f"Failed to setup Azure connection: {e}")
            raise
    
    def close(self):
        """Close the DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self._initialized = False


def get_azure_connection() -> DuckDBBlobConnection:
    """
    Get a DuckDB connection configured for Azure Blob Storage.
    
    Returns:
        Configured DuckDB connection manager
    """
    return DuckDBBlobConnection()


def test_blob_connection() -> bool:
    """
    Test connection to Azure Blob Storage.
    
    Returns:
        True if connection successful, False otherwise
    """
    if not USE_BLOB_STORAGE:
        logger.info("Blob storage disabled, skipping connection test")
        return True
        
    try:
        with get_azure_connection() as conn:
            # Try a simple query to test the connection
            test_query = "SELECT 1 as test"
            result = conn.execute(test_query).fetchone()
            
            if result and result[0] == 1:
                logger.info("DuckDB Azure connection test successful")
                return True
            else:
                logger.error("DuckDB Azure connection test failed")
                return False
                
    except Exception as e:
        logger.error(f"Azure connection test failed: {e}")
        return False


def check_blob_exists(blob_path: str) -> bool:
    """
    Check if a blob file exists.
    
    Args:
        blob_path: Relative path to blob file
        
    Returns:
        True if blob exists, False otherwise
    """
    if not USE_BLOB_STORAGE:
        return True  # Skip check for local files
        
    try:
        with get_azure_connection() as conn:
            blob_url = get_blob_url(blob_path)
            # Try to read just the first row to check existence
            query = f"SELECT * FROM '{blob_url}' LIMIT 1"
            conn.execute(query)
            return True
            
    except Exception as e:
        logger.warning(f"Blob not found or inaccessible: {blob_path} - {e}")
        return False


def get_blob_info(blob_path: str) -> dict:
    """
    Get information about a blob file.
    
    Args:
        blob_path: Relative path to blob file
        
    Returns:
        Dictionary with blob information
    """
    if not USE_BLOB_STORAGE:
        return {"type": "local", "exists": True}
        
    try:
        with get_azure_connection() as conn:
            blob_url = get_blob_url(blob_path)
            
            # Get row count (for CSVs)
            count_query = f"SELECT COUNT(*) as row_count FROM '{blob_url}'"
            row_count = conn.execute(count_query).fetchone()[0]
            
            return {
                "type": "blob",
                "exists": True,
                "row_count": row_count,
                "url": blob_url
            }
            
    except Exception as e:
        return {
            "type": "blob",
            "exists": False,
            "error": str(e)
        }


def execute_blob_query(query: str, description: str = "") -> Optional[List]:
    """
    Execute a query that may involve blob storage.
    
    Args:
        query: SQL query to execute
        description: Description for logging
        
    Returns:
        Query results if any, None for write operations
    """
    if description:
        logger.info(f"Executing: {description}")
    
    try:
        with get_azure_connection() as conn:
            start_time = time.time()
            
            if query.strip().upper().startswith('SELECT'):
                # For SELECT queries, return results
                result = conn.execute(query).fetchall()
                elapsed = time.time() - start_time
                logger.info(f"Query completed in {elapsed:.2f} seconds, {len(result)} rows")
                return result
            else:
                # For INSERT/COPY/etc, just execute
                conn.execute(query)
                elapsed = time.time() - start_time
                logger.info(f"Query completed in {elapsed:.2f} seconds")
                return None
                
    except Exception as e:
        logger.error(f"Query failed: {e}")
        if description:
            logger.error(f"Failed query: {description}")
        raise


def copy_blob_to_blob(source_path: str, dest_path: str, where_clause: str = "") -> None:
    """
    Copy data from one blob to another with optional filtering.
    
    Args:
        source_path: Source blob relative path
        dest_path: Destination blob relative path  
        where_clause: Optional WHERE clause for filtering
    """
    source_url = get_blob_url(source_path)
    dest_url = get_blob_url(dest_path)
    
    where_part = f"WHERE {where_clause}" if where_clause else ""
    
    query = f"""
    COPY (
        SELECT * FROM '{source_url}' 
        {where_part}
    ) TO '{dest_url}' (DELIMITER ';', HEADER)
    """
    
    description = f"Copy {source_path} -> {dest_path}"
    if where_clause:
        description += f" with filter: {where_clause[:50]}..."
        
    execute_blob_query(query, description)