#!/usr/bin/env python3
"""
Generate clean analysis-ready dataset from threshold analysis.
Creates the final historical country-level dataset for analysis.
"""
import logging
import pandas as pd
from pathlib import Path

from src.threshold_analyzer import ThresholdAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Generate the final analysis dataset."""
    logger.info("=== GENERATING ANALYSIS-READY DATASET ===")
    
    try:
        # Initialize analyzer with real population data
        analyzer = ThresholdAnalyzer(use_random_population=False)
        
        # Run full analysis
        logger.info("Running threshold analysis...")
        monthly_exposure = analyzer.run_full_analysis()
        
        # Create clean analysis dataset
        logger.info("Creating clean analysis dataset...")
        
        # Select and rename columns for analysis
        analysis_df = monthly_exposure[[
            'country', 'year_month', 'total_population',
            'pop_warning_1_plus', 'pct_warning_1_plus',
            'pop_warning_2_plus', 'pct_warning_2_plus', 
            'pop_warning_3_plus', 'pct_warning_3_plus',
            'pop_warning_4_plus', 'pct_warning_4_plus'
        ]].copy()
        
        # Convert year_month to proper date format
        analysis_df['date'] = pd.to_datetime(analysis_df['year_month'])
        analysis_df['year'] = analysis_df['date'].dt.year
        analysis_df['month'] = analysis_df['date'].dt.month
        
        # Round population numbers and percentages
        pop_cols = [col for col in analysis_df.columns if col.startswith('pop_')]
        pct_cols = [col for col in analysis_df.columns if col.startswith('pct_')]
        
        analysis_df[pop_cols] = analysis_df[pop_cols].round(0).astype(int)
        analysis_df[pct_cols] = analysis_df[pct_cols].round(2)
        
        # Sort by country and date
        analysis_df = analysis_df.sort_values(['country', 'date'])
        
        # Save clean dataset
        output_file = Path('data/processed/threshold_analysis/rosea_monthly_exposure_analysis.csv')
        analysis_df.to_csv(output_file, index=False)
        
        # Generate summary report
        logger.info("\n=== ANALYSIS DATASET SUMMARY ===")
        print(f"Dataset shape: {analysis_df.shape}")
        print(f"Date range: {analysis_df['date'].min().strftime('%Y-%m')} to {analysis_df['date'].max().strftime('%Y-%m')}")
        print(f"Countries: {analysis_df['country'].nunique()}")
        print(f"Total months: {analysis_df['year_month'].nunique()}")
        print(f"Output file: {output_file}")
        
        print("\nCountries included:")
        for country in sorted(analysis_df['country'].unique()):
            country_data = analysis_df[analysis_df['country'] == country]
            avg_total_pop = country_data['total_population'].mean()
            avg_warning2_pct = country_data['pct_warning_2_plus'].mean()
            print(f"  {country}: {avg_total_pop:,.0f} people, {avg_warning2_pct:.1f}% avg at Warning 2+")
        
        print("\nSample data (first 3 rows):")
        print(analysis_df[['country', 'year_month', 'total_population', 'pop_warning_2_plus', 'pct_warning_2_plus']].head(3))
        
        logger.info("Analysis dataset generation complete!")
        return analysis_df
        
    except Exception as e:
        logger.error(f"Analysis dataset generation failed: {e}")
        raise


if __name__ == "__main__":
    main()