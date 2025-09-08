#!/usr/bin/env python3
"""
Main script for running the complete ASAP threshold analysis pipeline.
"""
import sys
import logging

from src.asap.threshold_analyzer import ThresholdAnalyzer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run the complete ASAP threshold analysis pipeline."""
    try:
        logger.info("=== ASAP THRESHOLD ANALYSIS PIPELINE ===")
        
        # Initialize analyzer (will use real population data)
        analyzer = ThresholdAnalyzer(use_random_population=False)
        
        # Run complete analysis
        results = analyzer.run_full_analysis()
        
        # Print summary
        stats = analyzer.get_summary_stats()
        logger.info("=== PIPELINE COMPLETED SUCCESSFULLY ===")
        for key, value in stats.items():
            logger.info(f"{key}: {value}")
            
        return results
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()