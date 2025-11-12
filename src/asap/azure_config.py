"""
Azure Blob Storage configuration for ASAP indicator module.
Handles Azure connection settings and blob path management.
"""

import os
import logging
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Azure Storage Configuration
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "")
SAS_TOKEN = os.getenv("AZURE_SAS_TOKEN", "")
USE_BLOB_STORAGE = os.getenv("USE_BLOB_STORAGE", "false").lower() == "true"

# Container and project structure
CONTAINER = os.getenv("AZURE_CONTAINER", "projects")
PROJECT_PATH = os.getenv("AZURE_PROJECT_PATH", "ds-rosea-thresholds")

# Blob directory paths
BLOB_RAW_PATH = f"{PROJECT_PATH}/raw/asap"
BLOB_PROCESSED_PATH = f"{PROJECT_PATH}/processed/asap"

# File paths in blob storage
BLOB_WARNINGS_FILE = f"{BLOB_RAW_PATH}/warnings_l2_ts.csv"
BLOB_WORLDPOP_FILE = f"{BLOB_RAW_PATH}/worldpop_zonal_sum_rosea15_l2.csv"

BLOB_FILTERED_WARNINGS = f"{BLOB_PROCESSED_PATH}/warnings_filtered.csv"
BLOB_THRESHOLD_ANALYSIS_DIR = f"{BLOB_PROCESSED_PATH}/asap_warning_exposure"
BLOB_MONTHLY_EXPOSURE_FILE = (
    f"{BLOB_THRESHOLD_ANALYSIS_DIR}/monthly_exposure_crop_rangeland_warnings.csv"
)


def get_blob_url(relative_path: str) -> str:
    """
    Build full Azure blob URL with container.

    Args:
        relative_path: Path relative to container root

    Returns:
        Full Azure blob URL
    """
    return f"azure://{CONTAINER}/{relative_path}"


def get_blob_base_url() -> str:
    """Get base Azure blob URL for the project."""
    return f"azure://{CONTAINER}/{PROJECT_PATH}"


def validate_azure_config() -> bool:
    """
    Validate Azure configuration is complete.

    Returns:
        True if configuration is valid, False otherwise
    """
    if not USE_BLOB_STORAGE:
        return True

    missing_config = []

    if not STORAGE_ACCOUNT:
        missing_config.append("AZURE_STORAGE_ACCOUNT")
    if not SAS_TOKEN:
        missing_config.append("AZURE_SAS_TOKEN")

    if missing_config:
        logger.error(f"Missing Azure configuration: {', '.join(missing_config)}")  # noqa: E501
        logger.error("Please set these environment variables or create a .env file")  # noqa: E501
        return False

    logger.info(f"Azure config validated: {STORAGE_ACCOUNT}/{CONTAINER}/{PROJECT_PATH}")  # noqa: E501
    return True


def get_warnings_url() -> str:
    """Get URL for warnings data file."""
    return get_blob_url(BLOB_WARNINGS_FILE)


def get_worldpop_url() -> str:
    """Get URL for worldpop data file."""
    return get_blob_url(BLOB_WORLDPOP_FILE)


def get_filtered_warnings_url() -> str:
    """Get URL for filtered warnings output."""
    return get_blob_url(BLOB_FILTERED_WARNINGS)


def get_monthly_exposure_url() -> str:
    """Get URL for monthly exposure analysis output."""
    return get_blob_url(BLOB_MONTHLY_EXPOSURE_FILE)


def log_azure_info():
    """Log Azure configuration info for debugging."""
    if USE_BLOB_STORAGE:
        logger.info("=== AZURE BLOB CONFIGURATION ===")
        logger.info(f"Storage Account: {STORAGE_ACCOUNT}")
        logger.info(f"Container: {CONTAINER}")
        logger.info(f"Project Path: {PROJECT_PATH}")
        logger.info(f"Warnings URL: {get_warnings_url()}")
        logger.info(f"Worldpop URL: {get_worldpop_url()}")
        logger.info("=================================")
    else:
        logger.info("Using local file storage (USE_BLOB_STORAGE=false)")
