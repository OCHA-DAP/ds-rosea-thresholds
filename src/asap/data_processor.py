"""
Data processing module for combining warnings and population data.
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Optional

from .config import (
    FILTERED_WARNINGS_FILE, WORLDPOP_FILE, COMBINED_DATA_FILE,
    WARNINGS_SEPARATOR, WORLDPOP_SEPARATOR, POPULATION_COL,
    WARNINGS_COUNTRY_COL, WARNINGS_ADMIN2_COL,
    TARGET_COUNTRIES
)

logger = logging.getLogger(__name__)


class DataProcessor:
    """Class for processing and combining datasets."""
    
    def __init__(self):
        self.warnings_data: Optional[pd.DataFrame] = None
        self.worldpop_data: Optional[pd.DataFrame] = None
        self.combined_data: Optional[pd.DataFrame] = None
    
    def load_filtered_warnings(self) -> pd.DataFrame:
        """Load the filtered warnings data."""
        logger.info(f"Loading filtered warnings data from {FILTERED_WARNINGS_FILE}")
        
        if not FILTERED_WARNINGS_FILE.exists():
            raise FileNotFoundError(
                f"Filtered warnings file not found: {FILTERED_WARNINGS_FILE}\n"
                "Please run the filtering script first: python scripts/filter_warnings.py"
            )
        
        self.warnings_data = pd.read_csv(
            FILTERED_WARNINGS_FILE,
            sep=WARNINGS_SEPARATOR
        )
        
        logger.info(f"Loaded {len(self.warnings_data):,} warning records")
        return self.warnings_data
    
    def load_population_data(self) -> pd.DataFrame:
        """Load the population data."""
        logger.info(f"Loading population data from {WORLDPOP_FILE}")
        
        self.worldpop_data = pd.read_csv(
            WORLDPOP_FILE,
            sep=WORLDPOP_SEPARATOR
        )
        
        # Filter population data to target countries only
        self.worldpop_data = self.worldpop_data[
            self.worldpop_data['name0'].isin(TARGET_COUNTRIES)
        ]
        
        logger.info(f"Loaded {len(self.worldpop_data):,} population records")
        return self.worldpop_data
    
    def combine_datasets(self, how='left') -> pd.DataFrame:
        """
        Combine warnings and worldpop datasets.
        
        Args:
            how: How to join the datasets ('left', 'inner', 'outer', 'right')
        
        Returns:
            Combined DataFrame
        """
        if self.warnings_data is None:
            self.load_filtered_warnings()
        if self.worldpop_data is None:
            self.load_population_data()
        
        logger.info("Combining datasets...")
        
        # Merge on admin 2 level codes
        self.combined_data = pd.merge(
            self.warnings_data,
            self.worldpop_data,
            left_on=WARNINGS_ADMIN2_COL,
            right_on=WARNINGS_ADMIN2_COL,
            how=how,
            suffixes=('_warnings', '_population')
        )
        
        logger.info(f"Combined dataset contains {len(self.combined_data):,} records")
        return self.combined_data
    
    def save_combined_data(self, output_file: Optional[Path] = None):
        """Save combined data to CSV."""
        if self.combined_data is None:
            raise ValueError("No combined data to save. Run combine_datasets() first.")
        
        output_file = output_file or COMBINED_DATA_FILE
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.combined_data.to_csv(output_file, index=False)
        logger.info(f"Combined data saved to {output_file}")
    
    def get_data_summary(self) -> dict:
        """Get summary statistics of the datasets."""
        summary = {}
        
        if self.warnings_data is not None:
            summary['warnings'] = {
                'total_records': len(self.warnings_data),
                'countries': self.warnings_data[WARNINGS_COUNTRY_COL.strip('"')].nunique(),
                'date_range': {
                    'min': self.warnings_data['date'].min() if 'date' in self.warnings_data.columns else None,
                    'max': self.warnings_data['date'].max() if 'date' in self.warnings_data.columns else None
                }
            }
        
        if self.worldpop_data is not None:
            summary['worldpop'] = {
                'total_records': len(self.worldpop_data),
                'countries': self.worldpop_data[WORLDPOP_COUNTRY_COL].nunique(),
                'total_population': self.worldpop_data['population_sum_2020'].sum() if 'population_sum_2020' in self.worldpop_data.columns else None
            }
        
        if self.combined_data is not None:
            summary['combined'] = {
                'total_records': len(self.combined_data),
                'countries': len(set(self.combined_data[WARNINGS_COUNTRY_COL.strip('"')].unique()) & set(TARGET_COUNTRIES))
            }
        
        return summary
    
    def process_all(self) -> pd.DataFrame:
        """
        Complete processing pipeline: load, combine, and save data.
        
        Returns:
            Combined DataFrame
        """
        logger.info("Starting complete data processing pipeline...")
        
        self.load_filtered_warnings()
        self.load_population_data()
        self.combine_datasets()
        self.save_combined_data()
        
        summary = self.get_data_summary()
        logger.info(f"Processing complete. Summary: {summary}")
        
        return self.combined_data