#!/usr/bin/env python3
"""
Test script for threshold analysis pipeline with REAL population data.
"""
import logging
import pandas as pd

from src.threshold_analyzer import ThresholdAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Test the threshold analysis pipeline with real population data."""
    logger.info("=== TESTING WITH REAL POPULATION DATA ===")
    
    try:
        # Initialize analyzer with REAL population data
        analyzer = ThresholdAnalyzer(use_random_population=False)
        
        # Run full analysis
        monthly_exposure = analyzer.run_full_analysis()
        
        # Display sample results
        logger.info("\n=== REAL DATA RESULTS ===")
        print("First 5 country-months:")
        print(monthly_exposure.head())
        
        print(f"\nShape: {monthly_exposure.shape}")
        print(f"Columns: {list(monthly_exposure.columns)}")
        
        # Show summary by country with REAL populations
        print("\nCountry summary (REAL 2020 populations - average monthly at Warning Group 2+):")
        country_summary = monthly_exposure.groupby('country').agg({
            'total_population': 'mean',
            'pop_warning_2_plus': 'mean',
            'pct_warning_2_plus': 'mean'
        }).round(0)
        print(country_summary)
        
        # Compare total populations to see realistic numbers
        print(f"\nTotal population covered: {monthly_exposure['total_population'].iloc[0]:,.0f}")
        print(f"Average monthly population at Warning 2+: {monthly_exposure['pop_warning_2_plus'].mean():,.0f}")
        
        # Show recent trends for Angola
        print("\nAngola recent trends (last 12 months - REAL DATA):")
        angola_data = monthly_exposure[
            monthly_exposure['country'] == 'Angola'
        ].tail(12)[['year_month', 'total_population', 'pop_warning_2_plus', 'pct_warning_2_plus']]
        print(angola_data)
        
        logger.info("Real population test completed successfully!")
        return monthly_exposure
        
    except Exception as e:
        logger.error(f"Real population test failed: {e}")
        raise


if __name__ == "__main__":
    main()