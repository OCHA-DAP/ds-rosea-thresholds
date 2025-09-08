#!/usr/bin/env python3
"""
Main entry point for ds-rosea-thresholds analysis.
"""
import logging
from pathlib import Path

from src.asap.threshold_analyzer import ThresholdAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main analysis function."""
    logger.info("Starting ROSEA thresholds analysis")
    
    try:
        # Initialize threshold analyzer with real population data
        analyzer = ThresholdAnalyzer(use_random_population=False)
        
        # Run complete threshold analysis pipeline
        monthly_exposure = analyzer.run_full_analysis()
        
        # Display summary
        summary = analyzer.get_summary_stats()
        print("\n" + "="*60)
        print("ROSEA THRESHOLD ANALYSIS SUMMARY")
        print("="*60)
        
        print(f"Total country-months analyzed: {summary['total_country_months']:,}")
        print(f"Countries: {summary['countries']}")
        print(f"Time period: {summary['date_range']['start']} to {summary['date_range']['end']}")
        
        print(f"\nAverage monthly population exposure:")
        for threshold in [1, 2, 3, 4]:
            key = f'avg_pop_warning_{threshold}_plus'
            if key in summary:
                print(f"  Warning Group {threshold}+: {summary[key]:,} people")
        
        # Show top 5 countries by average Warning Group 2+ exposure
        print(f"\nTop countries by Warning Group 2+ exposure:")
        country_summary = monthly_exposure.groupby('country')['pct_warning_2_plus'].mean().sort_values(ascending=False)
        for country, pct in country_summary.head(5).items():
            pop = monthly_exposure[monthly_exposure['country'] == country]['pop_warning_2_plus'].mean()
            print(f"  {country}: {pct:.1f}% ({pop:,.0f} people)")
        
        print("="*60)
        logger.info("ROSEA threshold analysis complete!")
        
        return monthly_exposure
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        return None


if __name__ == "__main__":
    main()