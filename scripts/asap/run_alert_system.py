#!/usr/bin/env python3
"""
Script for running the ASAP alert system analysis.
"""
import sys
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.asap.alert_system import ASAPAlertSystem, AlertLevel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Run alert system analysis with different configurations."""
    try:
        logger.info("=== ASAP ALERT SYSTEM ANALYSIS ===")
        
        # Configuration 1: Standard thresholds, separate warning types
        logger.info("\n1. Running with STANDARD thresholds (Warning 1+ based)...")
        alert_system_standard = ASAPAlertSystem(
            use_conservative_thresholds=False,
            combine_warning_types=False
        )
        results_standard = alert_system_standard.run_full_alert_analysis()
        
        # Configuration 2: Conservative thresholds, separate warning types  
        logger.info("\n2. Running with CONSERVATIVE thresholds (Warning 2+ based)...")
        alert_system_conservative = ASAPAlertSystem(
            use_conservative_thresholds=True,
            combine_warning_types=False
        )
        results_conservative = alert_system_conservative.run_full_alert_analysis()
        
        # Configuration 3: Standard thresholds, combined warning types
        logger.info("\n3. Running with COMBINED warning types...")
        alert_system_combined = ASAPAlertSystem(
            use_conservative_thresholds=False,
            combine_warning_types=True
        )
        results_combined = alert_system_combined.run_full_alert_analysis()
        
        # Display current alert status for each configuration
        logger.info("\n=== CURRENT ALERT STATUS COMPARISON ===")
        
        configs = [
            ("Standard Thresholds", alert_system_standard),
            ("Conservative Thresholds", alert_system_conservative), 
            ("Combined Types", alert_system_combined)
        ]
        
        for config_name, system in configs:
            logger.info(f"\n{config_name}:")
            current_alerts = system.get_current_alerts()
            
            for warning_type in ['crop', 'range']:
                if f'{warning_type}_alert_level' not in current_alerts.columns:
                    continue
                    
                logger.info(f"\n  {warning_type.upper()} ALERTS:")
                
                for alert_level in AlertLevel:
                    countries = system.get_countries_by_alert_level(alert_level, warning_type)
                    if countries:
                        logger.info(f"    {alert_level.name}: {', '.join(countries)}")
            
            if 'combined_alert_level' in current_alerts.columns:
                logger.info(f"\n  COMBINED ALERTS:")
                for alert_level in AlertLevel:
                    countries = current_alerts[
                        current_alerts['combined_alert_level'] == alert_level.value
                    ]['country'].tolist()
                    if countries:
                        logger.info(f"    {alert_level.name}: {', '.join(sorted(countries))}")
        
        # Example: Get specific country history
        logger.info("\n=== EXAMPLE: ZAMBIA ALERT HISTORY (last 12 months) ===")
        zambia_history = alert_system_standard.get_country_alert_history('Zambia')
        if len(zambia_history) > 0:
            recent_zambia = zambia_history.tail(12)[['year_month', 'crop_alert_name', 'range_alert_name', 'crop_exposure_pct', 'range_exposure_pct']]
            print(recent_zambia.to_string(index=False))
        
        logger.info("\n=== ANALYSIS COMPLETED SUCCESSFULLY ===")
        return results_standard
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Alert analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()