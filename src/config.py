"""
Configuration file for ds-rosea-thresholds project.
Contains constants, file paths, and processing parameters.
"""
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Input file paths
WARNINGS_FILE = RAW_DATA_DIR / "warnings_l2_ts.csv"
WORLDPOP_FILE = RAW_DATA_DIR / "worldpop_unAdj2020_l2_ROSEA.csv"

# Output file paths
FILTERED_WARNINGS_FILE = PROCESSED_DATA_DIR / "warnings_filtered.csv"
COMBINED_DATA_FILE = PROCESSED_DATA_DIR / "combined_analysis_data.csv"

# Target countries for analysis
TARGET_COUNTRIES = [
    'Burundi', 'Comoros', 'Djibouti', 'Kenya', 'Malawi', 
    'Rwanda', 'Uganda', 'Tanzania', 'Zambia', 'Zimbabwe', 
    'Angola', 'Eswatini', 'Lesotho', 'Madagascar', 'Namibia'
]

# Processing parameters
WARNINGS_SEPARATOR = ';'  # Separator used in warnings CSV
WORLDPOP_SEPARATOR = ','  # Separator used in worldpop CSV

# Column names
WARNINGS_COUNTRY_COL = 'asap0_name'
WORLDPOP_COUNTRY_COL = 'ADM0_NAME'