"""
Configuration file for ASAP indicator module.
Contains constants, file paths, and processing parameters.
"""
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ASAP_RAW_DATA_DIR = DATA_DIR / "raw" / "asap"
ASAP_PROCESSED_DATA_DIR = DATA_DIR / "processed" / "asap"

# Input file paths
WARNINGS_FILE = ASAP_RAW_DATA_DIR / "warnings_l2_ts.csv"
WORLDPOP_FILE = ASAP_RAW_DATA_DIR / "worldpop_asap_l2_zmean.csv"

# Output file paths
FILTERED_WARNINGS_FILE = ASAP_PROCESSED_DATA_DIR / "warnings_filtered.csv"
COMBINED_DATA_FILE = ASAP_PROCESSED_DATA_DIR / "combined_analysis_data.csv"

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
WARNINGS_ADMIN2_COL = 'asap2_id'
POPULATION_COL = 'population_sum_2020'

# Warning level mappings and thresholds
WARNING_LEVEL_HIERARCHY = {
    'No warning': 0,
    'Warning group 1': 1, 
    'Warning group 2': 2,
    'Warning group 3': 3,
    'Warning group 4': 4,
    'Off season': -1,
    'No crop/rangeland': -2
}

# Reverse mapping for easy lookup
WARNING_HIERARCHY_REVERSE = {v: k for k, v in WARNING_LEVEL_HIERARCHY.items()}

# Threshold levels for analysis (ascending severity)
WARNING_THRESHOLDS = [1, 2, 3, 4]  # Warning group 1+, 2+, 3+, 4+

# Output file paths for threshold analysis
THRESHOLD_ANALYSIS_DIR = ASAP_PROCESSED_DATA_DIR / "threshold_analysis"
MONTHLY_EXPOSURE_FILE = THRESHOLD_ANALYSIS_DIR / "monthly_country_exposure.csv"
POPULATION_TEST_FILE = THRESHOLD_ANALYSIS_DIR / "admin2_test_populations.csv"

# Random population generation parameters (for testing)
RANDOM_POP_SEED = 42  # For reproducible testing
RANDOM_POP_MIN = 1000  # 1k minimum population
RANDOM_POP_MAX = 100000  # 100k maximum population