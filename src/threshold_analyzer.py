"""
Threshold analysis module for calculating population exposure at different warning levels.
Handles country-level monthly aggregations and threshold-based population calculations.
"""
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from .config import (
    FILTERED_WARNINGS_FILE, WARNINGS_SEPARATOR, WORLDPOP_FILE, POPULATION_COL,
    WARNINGS_ADMIN2_COL, WARNINGS_COUNTRY_COL,
    WARNING_LEVEL_HIERARCHY, WARNING_THRESHOLDS,
    THRESHOLD_ANALYSIS_DIR, MONTHLY_EXPOSURE_FILE, POPULATION_TEST_FILE,
    RANDOM_POP_SEED, RANDOM_POP_MIN, RANDOM_POP_MAX
)

logger = logging.getLogger(__name__)


class ThresholdAnalyzer:
    """Class for analyzing population exposure at different warning thresholds."""
    
    def __init__(self, use_random_population: bool = True):
        self.warnings_data: Optional[pd.DataFrame] = None
        self.population_data: Optional[pd.DataFrame] = None
        self.monthly_exposure: Optional[pd.DataFrame] = None
        self.use_random_population = use_random_population
        
        # Ensure output directory exists
        THRESHOLD_ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)
    
    def generate_random_populations(self) -> pd.DataFrame:
        """Generate random population data for testing purposes."""
        logger.info("Generating random population data for testing...")
        
        if self.warnings_data is None or self.warnings_data.empty:
            raise ValueError("Warning data must be loaded first")
        
        # Get unique admin2 IDs
        unique_admin2_ids = self.warnings_data[WARNINGS_ADMIN2_COL].unique()
        
        # Set seed for reproducibility
        np.random.seed(RANDOM_POP_SEED)
        
        # Generate random populations
        random_populations = np.random.randint(
            RANDOM_POP_MIN, 
            RANDOM_POP_MAX + 1, 
            size=len(unique_admin2_ids)
        )
        
        # Create population DataFrame
        pop_df = pd.DataFrame({
            WARNINGS_ADMIN2_COL: unique_admin2_ids,
            'population': random_populations,
            'data_source': 'random_test_data'
        })
        
        # Save for reference
        pop_df.to_csv(POPULATION_TEST_FILE, index=False)
        logger.info(f"Generated {len(pop_df):,} random populations")
        logger.info(f"Population range: {random_populations.min():,} - {random_populations.max():,}")
        
        return pop_df
    
    def load_warnings_data(self) -> pd.DataFrame:
        """Load and prepare warnings data."""
        logger.info(f"Loading warnings data from {FILTERED_WARNINGS_FILE}")
        
        self.warnings_data = pd.read_csv(FILTERED_WARNINGS_FILE, sep=WARNINGS_SEPARATOR)
        
        # Convert date column
        self.warnings_data['date'] = pd.to_datetime(self.warnings_data['date'])
        self.warnings_data['year_month'] = self.warnings_data['date'].dt.to_period('M')
        
        # Map warning levels to numeric hierarchy for threshold calculations
        self.warnings_data['warning_level_numeric'] = self.warnings_data['w_crop_gr'].map(
            WARNING_LEVEL_HIERARCHY
        )
        
        logger.info(f"Loaded {len(self.warnings_data):,} warning records")
        logger.info(f"Date range: {self.warnings_data['date'].min()} to {self.warnings_data['date'].max()}")
        
        return self.warnings_data
    
    def load_real_population_data(self) -> pd.DataFrame:
        """Load real population data from worldpop_asap_l2_zmean.csv."""
        logger.info(f"Loading population data from {WORLDPOP_FILE}")
        
        pop_df = pd.read_csv(WORLDPOP_FILE)
        
        # Select and rename columns to match expected format
        pop_df = pop_df[[WARNINGS_ADMIN2_COL, POPULATION_COL, 'name0', 'name2']].rename(
            columns={POPULATION_COL: 'population', 'name0': 'country', 'name2': 'admin2_name'}
        )
        
        # Add data source marker
        pop_df['data_source'] = 'worldpop_2020'
        
        logger.info(f"Loaded {len(pop_df):,} real population records")
        logger.info(f"Coverage: {pop_df['population'].sum():,.0f} total population")
        logger.info(f"Countries: {pop_df['country'].nunique()} unique countries")
        
        return pop_df
    
    def load_population_data(self, population_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Load population data (real or generated)."""
        if population_df is not None:
            self.population_data = population_df
            logger.info("Using provided population data")
        elif self.use_random_population:
            self.population_data = self.generate_random_populations()
        else:
            self.population_data = self.load_real_population_data()
        
        return self.population_data
    
    def calculate_monthly_exposure(self) -> pd.DataFrame:
        """Calculate monthly population exposure by country and warning level."""
        if self.warnings_data is None:
            self.load_warnings_data()
        if self.population_data is None:
            self.load_population_data()
        
        logger.info("Calculating monthly population exposure...")
        
        # Merge warnings with population data
        warnings_with_pop = pd.merge(
            self.warnings_data,
            self.population_data,
            on=WARNINGS_ADMIN2_COL,
            how='left'
        )
        
        # Filter out records with missing population (shouldn't happen with test data)
        missing_pop = warnings_with_pop['population'].isna().sum()
        if missing_pop > 0:
            logger.warning(f"Missing population data for {missing_pop:,} records")
            warnings_with_pop = warnings_with_pop.dropna(subset=['population'])
        
        # Group by country, month, and warning level
        monthly_groups = warnings_with_pop.groupby([
            WARNINGS_COUNTRY_COL, 'year_month', 'w_crop_gr', 'warning_level_numeric'
        ])['population'].sum().reset_index()
        
        # Calculate threshold-based exposures
        exposure_results = []
        
        for (country, month), group in monthly_groups.groupby([WARNINGS_COUNTRY_COL, 'year_month']):
            result = {
                'country': country,
                'year_month': str(month),
                'total_population': group['population'].sum()
            }
            
            # Calculate population at each warning level
            for warning_level, pop in zip(group['w_crop_gr'], group['population']):
                result[f'pop_{warning_level.lower().replace(" ", "_")}'] = result.get(
                    f'pop_{warning_level.lower().replace(" ", "_")}', 0
                ) + pop
            
            # Calculate threshold-based populations (X+ warning levels)
            for threshold in WARNING_THRESHOLDS:
                threshold_pop = group[
                    (group['warning_level_numeric'] >= threshold) & 
                    (group['warning_level_numeric'] >= 0)  # Exclude off-season/no-crop
                ]['population'].sum()
                
                result[f'pop_warning_{threshold}_plus'] = threshold_pop
                result[f'pct_warning_{threshold}_plus'] = (
                    threshold_pop / result['total_population'] * 100 
                    if result['total_population'] > 0 else 0
                )
            
            exposure_results.append(result)
        
        self.monthly_exposure = pd.DataFrame(exposure_results).fillna(0)
        
        logger.info(f"Calculated exposure for {len(self.monthly_exposure):,} country-months")
        return self.monthly_exposure
    
    def save_monthly_exposure(self, output_file: Optional[Path] = None):
        """Save monthly exposure data to CSV."""
        if self.monthly_exposure is None:
            raise ValueError("No monthly exposure data to save. Run calculate_monthly_exposure() first.")
        
        output_file = output_file or MONTHLY_EXPOSURE_FILE
        self.monthly_exposure.to_csv(output_file, index=False)
        logger.info(f"Monthly exposure data saved to {output_file}")
    
    def get_summary_stats(self) -> Dict:
        """Get summary statistics of the threshold analysis."""
        if self.monthly_exposure is None:
            return {}
        
        stats = {
            'total_country_months': len(self.monthly_exposure),
            'countries': self.monthly_exposure['country'].nunique(),
            'months': self.monthly_exposure['year_month'].nunique(),
            'date_range': {
                'start': self.monthly_exposure['year_month'].min(),
                'end': self.monthly_exposure['year_month'].max()
            }
        }
        
        # Calculate average populations at different thresholds
        for threshold in WARNING_THRESHOLDS:
            col = f'pop_warning_{threshold}_plus'
            if col in self.monthly_exposure.columns:
                stats[f'avg_pop_warning_{threshold}_plus'] = int(
                    self.monthly_exposure[col].mean()
                )
        
        return stats
    
    def run_full_analysis(self) -> pd.DataFrame:
        """Run the complete threshold analysis pipeline."""
        logger.info("Starting full threshold analysis...")
        
        self.load_warnings_data()
        self.load_population_data()
        self.calculate_monthly_exposure()
        self.save_monthly_exposure()
        
        stats = self.get_summary_stats()
        logger.info("Analysis complete!")
        logger.info(f"Summary: {stats}")
        
        return self.monthly_exposure