#!/usr/bin/env python3
"""
Main entry point for ds-rosea-thresholds analysis.
"""
import logging
from pathlib import Path

from src.data_processor import DataProcessor

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
        # Initialize data processor
        processor = DataProcessor()
        
        # Run complete processing pipeline
        combined_data = processor.process_all()
        
        # Display summary
        summary = processor.get_data_summary()
        print("\n" + "="*50)
        print("DATA PROCESSING SUMMARY")
        print("="*50)
        
        if 'warnings' in summary:
            print(f"Warnings data: {summary['warnings']['total_records']:,} records")
            print(f"Countries in warnings: {summary['warnings']['countries']}")
            if summary['warnings']['date_range']['min']:
                print(f"Date range: {summary['warnings']['date_range']['min']} to {summary['warnings']['date_range']['max']}")
        
        if 'worldpop' in summary:
            print(f"Population data: {summary['worldpop']['total_records']:,} records")
            print(f"Countries in population: {summary['worldpop']['countries']}")
            if summary['worldpop']['total_population']:
                print(f"Total population: {summary['worldpop']['total_population']:,.0f}")
        
        if 'combined' in summary:
            print(f"Combined data: {summary['combined']['total_records']:,} records")
            print(f"Countries matched: {summary['combined']['countries']}")
        
        print("="*50)
        logger.info("Analysis setup complete!")
        
        return combined_data
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        return None


if __name__ == "__main__":
    main()