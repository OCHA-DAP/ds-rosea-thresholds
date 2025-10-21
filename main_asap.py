#!/usr/bin/env python3
"""
Main entry point for ds-rosea-thresholds analysis.
"""
import logging
from pathlib import Path

from src.asap.asap_warning_exposure import AsapWarningExposure

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def main():
    """Main analysis function."""
    logger.info("Starting ROSEA thresholds analysis")

    try:
        # Initialize threshold analyzer with real population data
        analyzer = AsapWarningExposure(use_random_population=False)

        # Run complete threshold analysis pipeline
        monthly_exposure = analyzer.run_full_analysis()

        # Display summary
        summary = analyzer.get_summary_stats()
        print("\n" + "=" * 60)
        print("ROSEA THRESHOLD ANALYSIS SUMMARY")
        print("=" * 60)

        print(f"Total country-months analyzed: {summary['total_country_months']:,}")
        print(f"Countries: {summary['countries']}")
        print(
            f"Time period: {summary['date_range']['start']} to {summary['date_range']['end']}"
        )

        print("\nAverage monthly population exposure (crop warnings):")
        for threshold in [1, 2, 3, 4]:
            key = f"avg_pop_warning_{threshold}_plus"
            if key in summary.get('crop_warning_stats', {}):
                pop_val = summary['crop_warning_stats'][key]
                pct_key = f"avg_pct_warning_{threshold}_plus"
                pct_val = summary['crop_warning_stats'].get(pct_key, 0)
                print(f"  Warning Group {threshold}+: {pop_val:,} people "
                      f"({pct_val:.1f}%)")

        print("\nAverage monthly population exposure (range warnings):")
        for threshold in [1, 2, 3, 4]:
            key = f"avg_pop_warning_{threshold}_plus"
            if key in summary.get('range_warning_stats', {}):
                pop_val = summary['range_warning_stats'][key]
                pct_key = f"avg_pct_warning_{threshold}_plus"
                pct_val = summary['range_warning_stats'].get(pct_key, 0)
                print(f"  Warning Group {threshold}+: {pop_val:,} people "
                      f"({pct_val:.1f}%)")

        # Show top 5 countries by average Warning Group 2+ crop exposure
        print("\nTop countries by Warning Group 2+ crop exposure:")
        country_summary = (
            monthly_exposure.groupby("country")["crop_pct_warning_2_plus"]
            .mean()
            .sort_values(ascending=False)
        )
        for country, pct in country_summary.head(5).items():
            pop = monthly_exposure[monthly_exposure["country"] == country][
                "crop_pop_warning_2_plus"
            ].mean()
            print(f"  {country}: {pct:.1f}% ({pop:,.0f} people)")

        print("=" * 60)
        logger.info("ROSEA threshold analysis complete!")

        return monthly_exposure

    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        return None


if __name__ == "__main__":
    main()
