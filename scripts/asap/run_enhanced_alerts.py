#!/usr/bin/env python3
"""
Script for running the enhanced ASAP alert system comparison.
"""
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.asap.enhanced_alert_system import EnhancedAlertSystem

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run enhanced alert system comparison."""
    try:
        logger.info("=== ENHANCED ASAP ALERT SYSTEM COMPARISON ===")
        
        # Initialize system
        alert_system = EnhancedAlertSystem()
        
        # Generate alert comparisons
        logger.info("Generating alert comparisons with all methodologies...")
        alerts_df = alert_system.save_alerts_comparison()
        
        # Get summary statistics
        summary = alert_system.get_methodology_summary(alerts_df)
        
        logger.info("=== METHODOLOGY COMPARISON RESULTS ===")
        
        for methodology, stats in summary.items():
            logger.info(f"\n{methodology}:")
            logger.info(f"  Total Alerts: {stats['total_alerts']:,}")
            logger.info(f"  Average Score: {stats['avg_score']:.1f}")
            
            severe_pct = stats['alert_percentages']['severe_warning']
            moderate_pct = stats['alert_percentages']['moderate_warning'] 
            light_pct = stats['alert_percentages']['light_warning']
            none_pct = stats['alert_percentages']['no_warning']
            
            logger.info(f"  Severe Warnings: {severe_pct:.1f}%")
            logger.info(f"  Moderate Warnings: {moderate_pct:.1f}%")
            logger.info(f"  Light Warnings: {light_pct:.1f}%")
            logger.info(f"  No Warnings: {none_pct:.1f}%")
        
        # Show recent severe alerts by methodology
        logger.info("\n=== RECENT SEVERE ALERTS (2023+) ===")
        
        recent_data = alerts_df[alerts_df['year_month'] >= '2023-01']
        
        for methodology in alerts_df['methodology'].unique():
            method_data = recent_data[recent_data['methodology'] == methodology]
            severe_alerts = method_data[method_data['alert_level'] >= 3]
            
            if len(severe_alerts) > 0:
                logger.info(f"\n{methodology} - Recent Severe Alerts:")
                
                # Group by country and count
                severe_by_country = severe_alerts.groupby('country').size().sort_values(ascending=False)
                
                for country, count in severe_by_country.head(5).items():
                    # Get most recent severe alert details
                    recent_severe = severe_alerts[
                        severe_alerts['country'] == country
                    ].sort_values('year_month').iloc[-1]
                    
                    logger.info(f"  {country}: {count} severe alerts "
                               f"(latest: {recent_severe['year_month']}, "
                               f"{recent_severe['exposure_pct']:.1f}% exposure)")
            else:
                logger.info(f"\n{methodology}: No severe alerts in 2023+")
        
        # Methodology agreement analysis
        logger.info("\n=== METHODOLOGY AGREEMENT ANALYSIS ===")
        
        # Create comparison matrix
        comparison = alerts_df.pivot_table(
            index=['country', 'year_month', 'warning_type'],
            columns='methodology', 
            values='alert_level',
            aggfunc='first'
        ).reset_index()
        
        methods = ['Simple_Percentile', 'Seasonal_Adjusted', 'Multi_Factor']
        
        for i in range(len(methods)):
            for j in range(i+1, len(methods)):
                method1, method2 = methods[i], methods[j]
                
                if method1 in comparison.columns and method2 in comparison.columns:
                    agreement = (comparison[method1] == comparison[method2]).mean() * 100
                    logger.info(f"{method1} vs {method2}: {agreement:.1f}% agreement")
        
        logger.info("\n=== ANALYSIS COMPLETED SUCCESSFULLY ===")
        logger.info("Results saved to data/processed/asap/threshold_analysis/enhanced_alerts/")
        logger.info("Run the Quarto chapter for detailed visualizations:")
        logger.info("  quarto render book_rosea_thresholds/03_alert_system_comparison.qmd")
        
        return alerts_df
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Enhanced alert analysis failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()