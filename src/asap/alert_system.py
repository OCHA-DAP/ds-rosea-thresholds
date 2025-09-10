"""
Alert system module for ASAP threshold analysis.
Provides country-level monthly alert status based on population exposure.
"""
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, List, Optional
from enum import Enum

from .config import THRESHOLD_ANALYSIS_DIR, MONTHLY_EXPOSURE_FILE

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert level enumeration."""
    NO_WARNING = 0
    LIGHT_WARNING = 1
    MODERATE_WARNING = 2
    SEVERE_WARNING = 3


class AlertThresholdConfig:
    """Configuration for alert thresholds based on percentile analysis."""

    def __init__(self):
        # Thresholds based on percentile analysis of historical data
        # These represent exposure percentage thresholds for each alert level

        # CROP WARNING THRESHOLDS (based on Warning 1+ exposure percentages)
        # 75th percentile: 19.41%, 85th: 34.59%, 95th: 65.69%
        self.crop_thresholds = {
            AlertLevel.NO_WARNING: 0.0,  # 0-19% exposure
            AlertLevel.LIGHT_WARNING: 19.0,  # 19-35% (75th percentile)
            AlertLevel.MODERATE_WARNING: 35.0,  # 35-66% (85th percentile)
            AlertLevel.SEVERE_WARNING: 66.0  # 66%+ exposure (95th percentile)
        }

        # RANGE WARNING THRESHOLDS (based on Warning 1+ exposure percentages)
        # 75th percentile: 23.08%, 85th: 42.08%, 95th: 75.97%
        self.range_thresholds = {
            AlertLevel.NO_WARNING: 0.0,  # 0-23% exposure
            AlertLevel.LIGHT_WARNING: 23.0,  # 23-42% (75th percentile)
            AlertLevel.MODERATE_WARNING: 42.0,  # 42-76% (85th percentile)
            AlertLevel.SEVERE_WARNING: 76.0  # 76%+ exposure (95th percentile)
        }

        # Alternative: Use Warning 2+ thresholds for more conservative alerts
        self.crop_thresholds_conservative = {
            AlertLevel.NO_WARNING: 0.0,
            AlertLevel.LIGHT_WARNING: 7.0,  # 75th percentile for Warning 2+
            AlertLevel.MODERATE_WARNING: 17.0,  # 85th percentile
            AlertLevel.SEVERE_WARNING: 46.0  # 95th percentile for Warning 2+
        }

        self.range_thresholds_conservative = {
            AlertLevel.NO_WARNING: 0.0,
            AlertLevel.LIGHT_WARNING: 8.0,  # 75th percentile for Warning 2+
            AlertLevel.MODERATE_WARNING: 19.0,  # 85th percentile
            AlertLevel.SEVERE_WARNING: 48.0  # 95th percentile for Warning 2+
        }


class ASAPAlertSystem:
    """Main alert system class for monitoring population exposure."""
    
    def __init__(self,
                 use_conservative_thresholds: bool = False,
                 combine_warning_types: bool = False):
        """
        Initialize the alert system.

        Args:
            use_conservative_thresholds: Use Warning 2+ thresholds
            combine_warning_types: Combine crop and range for unified alerts
        """
        self.config = AlertThresholdConfig()
        self.use_conservative = use_conservative_thresholds
        self.combine_types = combine_warning_types
        
        self.exposure_data: Optional[pd.DataFrame] = None
        self.alerts_data: Optional[pd.DataFrame] = None
        
        # Ensure output directory exists
        self.output_dir = THRESHOLD_ANALYSIS_DIR / "alerts"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_exposure_data(self, file_path: Optional[Path] = None) -> pd.DataFrame:
        """Load monthly exposure data."""
        file_path = file_path or MONTHLY_EXPOSURE_FILE
        
        if not file_path.exists():
            raise FileNotFoundError(
                f"Exposure data not found: {file_path}\n"
                "Please run threshold analysis first."
            )
        
        self.exposure_data = pd.read_csv(file_path)
        logger.info(f"Loaded {len(self.exposure_data):,} exposure records")
        
        # Convert year_month to datetime for easier analysis
        self.exposure_data['date'] = pd.to_datetime(self.exposure_data['year_month'])
        
        return self.exposure_data
    
    def get_thresholds(self, warning_type: str) -> Dict[AlertLevel, float]:
        """Get thresholds for a specific warning type."""
        if self.use_conservative:
            if warning_type == 'crop':
                return self.config.crop_thresholds_conservative
            else:
                return self.config.range_thresholds_conservative
        else:
            if warning_type == 'crop':
                return self.config.crop_thresholds
            else:
                return self.config.range_thresholds
    
    def calculate_alert_level(self, exposure_pct: float, warning_type: str) -> AlertLevel:
        """Calculate alert level based on exposure percentage."""
        thresholds = self.get_thresholds(warning_type)
        
        if exposure_pct >= thresholds[AlertLevel.SEVERE_WARNING]:
            return AlertLevel.SEVERE_WARNING
        elif exposure_pct >= thresholds[AlertLevel.MODERATE_WARNING]:
            return AlertLevel.MODERATE_WARNING
        elif exposure_pct >= thresholds[AlertLevel.LIGHT_WARNING]:
            return AlertLevel.LIGHT_WARNING
        else:
            return AlertLevel.NO_WARNING
    
    def calculate_combined_alert_level(self, crop_pct: float, range_pct: float) -> AlertLevel:
        """Calculate combined alert level using maximum of crop and range."""
        crop_alert = self.calculate_alert_level(crop_pct, 'crop')
        range_alert = self.calculate_alert_level(range_pct, 'range')
        
        # Return the higher alert level
        return max(crop_alert, range_alert, key=lambda x: x.value)
    
    def generate_monthly_alerts(self) -> pd.DataFrame:
        """Generate monthly alert status for all countries."""
        if self.exposure_data is None:
            self.load_exposure_data()
        
        logger.info("Generating monthly alerts...")
        
        alerts = []
        
        for _, row in self.exposure_data.iterrows():
            country = row['country']
            year_month = row['year_month']
            date = row['date']
            
            # Get exposure percentages
            crop_exposure = row['crop_pct_warning_1_plus']
            range_exposure = row['range_pct_warning_1_plus']
            
            # Calculate individual alert levels
            crop_alert = self.calculate_alert_level(crop_exposure, 'crop')
            range_alert = self.calculate_alert_level(range_exposure, 'range')
            
            alert_record = {
                'country': country,
                'year_month': year_month,
                'date': date,
                'crop_exposure_pct': crop_exposure,
                'range_exposure_pct': range_exposure,
                'crop_alert_level': crop_alert.value,
                'crop_alert_name': crop_alert.name,
                'range_alert_level': range_alert.value,
                'range_alert_name': range_alert.name,
                'total_population': row['total_population']
            }
            
            # Add combined alert if requested
            if self.combine_types:
                combined_alert = self.calculate_combined_alert_level(crop_exposure, range_exposure)
                alert_record.update({
                    'combined_alert_level': combined_alert.value,
                    'combined_alert_name': combined_alert.name
                })
            
            alerts.append(alert_record)
        
        self.alerts_data = pd.DataFrame(alerts)
        logger.info(f"Generated {len(self.alerts_data):,} alert records")
        
        return self.alerts_data
    
    def get_current_alerts(self, reference_date: Optional[str] = None) -> pd.DataFrame:
        """Get current alert status (most recent month or specified date)."""
        if self.alerts_data is None:
            self.generate_monthly_alerts()
        
        if reference_date:
            # Get alerts for specific month
            current_alerts = self.alerts_data[
                self.alerts_data['year_month'] == reference_date
            ].copy()
        else:
            # Get most recent month for each country
            latest_date = self.alerts_data['date'].max()
            current_alerts = self.alerts_data[
                self.alerts_data['date'] == latest_date
            ].copy()
        
        return current_alerts.sort_values('country')
    
    def get_alert_summary(self) -> Dict:
        """Get summary statistics of alert system."""
        if self.alerts_data is None:
            self.generate_monthly_alerts()
        
        total_records = len(self.alerts_data)
        date_range = {
            'start': self.alerts_data['year_month'].min(),
            'end': self.alerts_data['year_month'].max()
        }
        
        summary = {
            'total_records': total_records,
            'countries': self.alerts_data['country'].nunique(),
            'date_range': date_range,
            'threshold_type': 'conservative' if self.use_conservative else 'standard',
            'combines_warning_types': self.combine_types
        }
        
        # Alert level distribution for crop and range
        for warning_type in ['crop', 'range']:
            alert_col = f'{warning_type}_alert_level'
            alert_dist = self.alerts_data[alert_col].value_counts().sort_index()
            
            summary[f'{warning_type}_alert_distribution'] = {
                'no_warning': alert_dist.get(0, 0),
                'light_warning': alert_dist.get(1, 0),
                'moderate_warning': alert_dist.get(2, 0),
                'severe_warning': alert_dist.get(3, 0)
            }
            
            # Calculate percentages
            total = alert_dist.sum()
            summary[f'{warning_type}_alert_percentages'] = {
                'no_warning': alert_dist.get(0, 0) / total * 100,
                'light_warning': alert_dist.get(1, 0) / total * 100,
                'moderate_warning': alert_dist.get(2, 0) / total * 100,
                'severe_warning': alert_dist.get(3, 0) / total * 100
            }
        
        return summary
    
    def save_alerts(self, output_file: Optional[Path] = None):
        """Save alert data to CSV."""
        if self.alerts_data is None:
            raise ValueError("No alert data to save. Run generate_monthly_alerts() first.")
        
        threshold_suffix = "conservative" if self.use_conservative else "standard"
        combined_suffix = "combined" if self.combine_types else "separate"
        
        if output_file is None:
            filename = f"monthly_alerts_{threshold_suffix}_{combined_suffix}.csv"
            output_file = self.output_dir / filename
        
        self.alerts_data.to_csv(output_file, index=False)
        logger.info(f"Alert data saved to {output_file}")
    
    def get_countries_by_alert_level(self, alert_level: AlertLevel, 
                                   warning_type: str = 'crop',
                                   reference_date: Optional[str] = None) -> List[str]:
        """Get list of countries at specific alert level."""
        current_alerts = self.get_current_alerts(reference_date)
        
        alert_col = f'{warning_type}_alert_level'
        countries = current_alerts[
            current_alerts[alert_col] == alert_level.value
        ]['country'].tolist()
        
        return sorted(countries)
    
    def get_country_alert_history(self, country: str) -> pd.DataFrame:
        """Get alert history for a specific country."""
        if self.alerts_data is None:
            self.generate_monthly_alerts()
        
        country_data = self.alerts_data[
            self.alerts_data['country'] == country
        ].copy().sort_values('date')
        
        return country_data
    
    def run_full_alert_analysis(self) -> pd.DataFrame:
        """Run complete alert analysis pipeline."""
        logger.info("Starting full alert analysis...")
        
        self.load_exposure_data()
        self.generate_monthly_alerts()
        self.save_alerts()
        
        summary = self.get_alert_summary()
        logger.info("Alert analysis complete!")
        logger.info(f"Summary: {summary}")
        
        return self.alerts_data