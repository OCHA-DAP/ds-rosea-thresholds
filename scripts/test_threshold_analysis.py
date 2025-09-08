#!/usr/bin/env python3
"""
Test script for threshold analysis pipeline with random population data.
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
    """Test the complete threshold analysis pipeline."""
    logger.info("=== TESTING THRESHOLD ANALYSIS PIPELINE ===")
    
    try:
        # Initialize analyzer with random population
        analyzer = ThresholdAnalyzer(use_random_population=True)
        
        # Run full analysis
        monthly_exposure = analyzer.run_full_analysis()
        
        # Display sample results
        logger.info("\n=== SAMPLE RESULTS ===")
        print("First 5 country-months:")
        print(monthly_exposure.head())
        
        print(f"\nColumns: {list(monthly_exposure.columns)}")
        
        # Show summary by country
        print("\nCountry summary (average monthly populations at Warning Group 2+):")
        country_summary = monthly_exposure.groupby('country').agg({
            'total_population': 'mean',
            'pop_warning_2_plus': 'mean',
            'pct_warning_2_plus': 'mean'
        }).round(0)
        print(country_summary)
        
        # Show temporal trends for one country
        sample_country = monthly_exposure['country'].iloc[0]
        print(f"\nTemporal trend for {sample_country} (last 12 months):")
        country_data = monthly_exposure[
            monthly_exposure['country'] == sample_country
        ].tail(12)[['year_month', 'pop_warning_2_plus', 'pct_warning_2_plus']]
        print(country_data)
        
        logger.info("Test completed successfully!")
        return monthly_exposure
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise


if __name__ == "__main__":
    main()