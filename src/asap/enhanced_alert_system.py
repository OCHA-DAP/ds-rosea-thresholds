"""
Enhanced alert system with multiple threshold methodologies.
Compares simple percentile vs seasonal-adjusted vs multi-factor approaches.
"""
import pandas as pd
import numpy as np
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from enum import Enum
from dataclasses import dataclass

from .config import THRESHOLD_ANALYSIS_DIR
from .azure_config import USE_BLOB_STORAGE, get_monthly_exposure_url
from .blob_utils import get_azure_connection

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """Alert level enumeration."""
    NO_WARNING = 0
    LIGHT_WARNING = 1
    MODERATE_WARNING = 2
    SEVERE_WARNING = 3


@dataclass
class AlertResult:
    """Container for alert calculation results."""
    country: str
    year_month: str
    month: int
    warning_type: str
    exposure_pct: float
    exposure_pop_millions: float
    alert_level: AlertLevel
    alert_score: float
    methodology: str


class SeasonalThresholdConfig:
    """Seasonal-adjusted threshold configuration."""
    
    def __init__(self):
        # Based on seasonal analysis:
        # Low season (Jul-Sep): mean ~2-5%, std ~7-9%
        # High season (Nov-Apr): mean ~20-25%, std ~27-29%
        
        self.crop_thresholds = {
            # Low season months (July, August, September)
            'low_season': {
                'months': [7, 8, 9],
                'thresholds': {
                    AlertLevel.NO_WARNING: 0.0,
                    AlertLevel.LIGHT_WARNING: 5.0,    # Above typical low season
                    AlertLevel.MODERATE_WARNING: 15.0,  # Well above normal
                    AlertLevel.SEVERE_WARNING: 30.0   # Extreme for low season
                }
            },
            # High season months (November through April) 
            'high_season': {
                'months': [11, 12, 1, 2, 3, 4],
                'thresholds': {
                    AlertLevel.NO_WARNING: 0.0,
                    AlertLevel.LIGHT_WARNING: 30.0,   # Above typical high season
                    AlertLevel.MODERATE_WARNING: 50.0, # Well above normal
                    AlertLevel.SEVERE_WARNING: 80.0   # Crisis level (like Burundi)
                }
            },
            # Transition months (May, June, October)
            'transition': {
                'months': [5, 6, 10],
                'thresholds': {
                    AlertLevel.NO_WARNING: 0.0,
                    AlertLevel.LIGHT_WARNING: 15.0,
                    AlertLevel.MODERATE_WARNING: 35.0,
                    AlertLevel.SEVERE_WARNING: 60.0
                }
            }
        }
        
        # Similar pattern for rangeland (slightly higher baseline)
        self.range_thresholds = {
            'low_season': {
                'months': [7, 8, 9],
                'thresholds': {
                    AlertLevel.NO_WARNING: 0.0,
                    AlertLevel.LIGHT_WARNING: 8.0,
                    AlertLevel.MODERATE_WARNING: 20.0,
                    AlertLevel.SEVERE_WARNING: 35.0
                }
            },
            'high_season': {
                'months': [11, 12, 1, 2, 3, 4],
                'thresholds': {
                    AlertLevel.NO_WARNING: 0.0,
                    AlertLevel.LIGHT_WARNING: 35.0,
                    AlertLevel.MODERATE_WARNING: 55.0,
                    AlertLevel.SEVERE_WARNING: 85.0
                }
            },
            'transition': {
                'months': [5, 6, 10],
                'thresholds': {
                    AlertLevel.NO_WARNING: 0.0,
                    AlertLevel.LIGHT_WARNING: 20.0,
                    AlertLevel.MODERATE_WARNING: 40.0,
                    AlertLevel.SEVERE_WARNING: 65.0
                }
            }
        }


class EnhancedAlertSystem:
    """Enhanced alert system with multiple methodologies."""
    
    def __init__(self):
        self.seasonal_config = SeasonalThresholdConfig()
        self.exposure_data: Optional[pd.DataFrame] = None
        
        # Output directory
        self.output_dir = THRESHOLD_ANALYSIS_DIR / "enhanced_alerts"
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_exposure_data(self) -> pd.DataFrame:
        """Load exposure data from blob storage or local file."""
        if USE_BLOB_STORAGE:
            return self._load_from_blob()
        else:
            return self._load_from_local()
    
    def _load_from_blob(self) -> pd.DataFrame:
        """Load from Azure blob storage."""
        exposure_url = get_monthly_exposure_url()
        logger.info(f"Loading exposure data from blob: {exposure_url}")
        
        query = f"SELECT * FROM '{exposure_url}'"
        
        try:
            with get_azure_connection() as conn:
                result = conn.execute(query).fetchdf()
                self.exposure_data = self._process_exposure_data(result)
                return self.exposure_data
        except Exception as e:
            logger.error(f"Failed to load from blob: {e}")
            raise
    
    def _load_from_local(self) -> pd.DataFrame:
        """Load from local file."""
        local_file = (THRESHOLD_ANALYSIS_DIR / 
                     "monthly_exposure_dual_warnings.csv")
        
        if not local_file.exists():
            # Fall back to original file
            local_file = THRESHOLD_ANALYSIS_DIR / "monthly_country_exposure.csv"
            
        logger.info(f"Loading exposure data from: {local_file}")
        data = pd.read_csv(local_file)
        self.exposure_data = self._process_exposure_data(data)
        return self.exposure_data
    
    def _process_exposure_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and prepare exposure data."""
        # Convert date columns
        df['date'] = pd.to_datetime(df['year_month'])
        df['month'] = df['date'].dt.month
        df['year'] = df['date'].dt.year
        
        # Calculate population in millions
        for prefix in ['crop', 'range']:
            pop_col = f'{prefix}_pop_warning_1_plus'
            if pop_col in df.columns:
                df[f'{prefix}_pop_1_plus_millions'] = df[pop_col] / 1e6
            
        logger.info(f"Processed {len(df):,} exposure records")
        return df
    
    def get_seasonal_thresholds(self, month: int, 
                              warning_type: str) -> Dict[AlertLevel, float]:
        """Get seasonal thresholds for given month and warning type."""
        thresholds_config = getattr(self.seasonal_config, 
                                   f'{warning_type}_thresholds')
        
        # Determine season
        for season_name, season_config in thresholds_config.items():
            if month in season_config['months']:
                return season_config['thresholds']
        
        # Default to transition if not found
        return thresholds_config['transition']['thresholds']
    
    def calculate_simple_percentile_alert(self, exposure_pct: float, 
                                        warning_type: str) -> AlertLevel:
        """Simple percentile-based alert (75th, 85th, 95th percentiles)."""
        if warning_type == 'crop':
            thresholds = {
                AlertLevel.NO_WARNING: 0.0,
                AlertLevel.LIGHT_WARNING: 19.0,  # 75th percentile
                AlertLevel.MODERATE_WARNING: 35.0,  # 85th percentile  
                AlertLevel.SEVERE_WARNING: 66.0   # 95th percentile
            }
        else:  # range
            thresholds = {
                AlertLevel.NO_WARNING: 0.0,
                AlertLevel.LIGHT_WARNING: 23.0,  # 75th percentile
                AlertLevel.MODERATE_WARNING: 42.0,  # 85th percentile
                AlertLevel.SEVERE_WARNING: 76.0   # 95th percentile
            }
        
        for level in reversed(list(AlertLevel)):
            if exposure_pct >= thresholds[level]:
                return level
        return AlertLevel.NO_WARNING
    
    def calculate_seasonal_alert(self, exposure_pct: float, month: int,
                               warning_type: str) -> AlertLevel:
        """Seasonal-adjusted alert calculation."""
        thresholds = self.get_seasonal_thresholds(month, warning_type)
        
        for level in reversed(list(AlertLevel)):
            if exposure_pct >= thresholds[level]:
                return level
        return AlertLevel.NO_WARNING
    
    def calculate_multi_factor_score(self, exposure_pct: float, 
                                   exposure_pop_millions: float,
                                   month: int, country: str,
                                   warning_type: str) -> Tuple[float, AlertLevel]:
        """Multi-factor alert score incorporating multiple dimensions."""
        
        # Base score from seasonal-adjusted exposure percentage
        seasonal_alert = self.calculate_seasonal_alert(exposure_pct, month, 
                                                      warning_type)
        base_score = seasonal_alert.value * 25  # Scale to 0-75
        
        # Population impact bonus (up to +20 points)
        # Countries with >10M exposed get significant bonus
        pop_bonus = min(exposure_pop_millions * 2, 20)
        
        # Persistence bonus would go here (requires time series analysis)
        # For now, just placeholder
        persistence_bonus = 0
        
        # Chronic exposure countries get penalty to avoid alert fatigue
        # (Angola, Zimbabwe, Eswatini have high chronic exposure)
        chronic_countries = ['Angola', 'Zimbabwe', 'Eswatini']
        chronic_penalty = -5 if country in chronic_countries else 0
        
        # Final composite score
        composite_score = (base_score + pop_bonus + 
                         persistence_bonus + chronic_penalty)
        
        # Convert to alert level
        if composite_score >= 80:
            alert_level = AlertLevel.SEVERE_WARNING
        elif composite_score >= 60:
            alert_level = AlertLevel.MODERATE_WARNING
        elif composite_score >= 40:
            alert_level = AlertLevel.LIGHT_WARNING
        else:
            alert_level = AlertLevel.NO_WARNING
            
        return composite_score, alert_level
    
    def generate_alerts_comparison(self) -> pd.DataFrame:
        """Generate alerts using all methodologies for comparison."""
        if self.exposure_data is None:
            self.load_exposure_data()
            
        logger.info("Generating alerts with all methodologies...")
        
        results = []
        
        for _, row in self.exposure_data.iterrows():
            country = row['country']
            year_month = row['year_month'] 
            month = row['month']
            
            for warning_type in ['crop', 'range']:
                exposure_pct_col = f'{warning_type}_pct_warning_1_plus'
                exposure_pop_col = f'{warning_type}_pop_1_plus_millions'
                
                # Skip if columns don't exist
                if (exposure_pct_col not in row or 
                    exposure_pop_col not in row):
                    continue
                    
                exposure_pct = row[exposure_pct_col]
                exposure_pop = row.get(exposure_pop_col, 0)
                
                # Method 1: Simple Percentile
                percentile_alert = self.calculate_simple_percentile_alert(
                    exposure_pct, warning_type)
                
                results.append(AlertResult(
                    country=country,
                    year_month=year_month,
                    month=month,
                    warning_type=warning_type,
                    exposure_pct=exposure_pct,
                    exposure_pop_millions=exposure_pop,
                    alert_level=percentile_alert,
                    alert_score=percentile_alert.value * 25,
                    methodology='Simple_Percentile'
                ))
                
                # Method 2: Seasonal Adjustment
                seasonal_alert = self.calculate_seasonal_alert(
                    exposure_pct, month, warning_type)
                
                results.append(AlertResult(
                    country=country,
                    year_month=year_month,
                    month=month,
                    warning_type=warning_type,
                    exposure_pct=exposure_pct,
                    exposure_pop_millions=exposure_pop,
                    alert_level=seasonal_alert,
                    alert_score=seasonal_alert.value * 25,
                    methodology='Seasonal_Adjusted'
                ))
                
                # Method 3: Multi-Factor
                multi_score, multi_alert = self.calculate_multi_factor_score(
                    exposure_pct, exposure_pop, month, country, warning_type)
                
                results.append(AlertResult(
                    country=country,
                    year_month=year_month,
                    month=month,
                    warning_type=warning_type,
                    exposure_pct=exposure_pct,
                    exposure_pop_millions=exposure_pop,
                    alert_level=multi_alert,
                    alert_score=multi_score,
                    methodology='Multi_Factor'
                ))
        
        # Convert to DataFrame
        alert_df = pd.DataFrame([
            {
                'country': r.country,
                'year_month': r.year_month,
                'month': r.month,
                'warning_type': r.warning_type,
                'exposure_pct': r.exposure_pct,
                'exposure_pop_millions': r.exposure_pop_millions,
                'alert_level': r.alert_level.value,
                'alert_name': r.alert_level.name,
                'alert_score': r.alert_score,
                'methodology': r.methodology
            }
            for r in results
        ])
        
        logger.info(f"Generated {len(alert_df):,} alert comparisons")
        return alert_df
    
    def save_alerts_comparison(self, output_file: Optional[Path] = None):
        """Save alert comparison results."""
        alerts_df = self.generate_alerts_comparison()
        
        if output_file is None:
            output_file = self.output_dir / "alert_methodologies_comparison.csv"
            
        alerts_df.to_csv(output_file, index=False)
        logger.info(f"Alert comparison saved to: {output_file}")
        return alerts_df
    
    def get_methodology_summary(self, alerts_df: pd.DataFrame) -> Dict:
        """Get summary statistics comparing methodologies."""
        summary = {}
        
        for methodology in alerts_df['methodology'].unique():
            method_data = alerts_df[alerts_df['methodology'] == methodology]
            
            alert_dist = method_data['alert_level'].value_counts().sort_index()
            total = len(method_data)
            
            summary[methodology] = {
                'total_alerts': total,
                'alert_distribution': {
                    'no_warning': alert_dist.get(0, 0),
                    'light_warning': alert_dist.get(1, 0),
                    'moderate_warning': alert_dist.get(2, 0),
                    'severe_warning': alert_dist.get(3, 0)
                },
                'alert_percentages': {
                    'no_warning': alert_dist.get(0, 0) / total * 100,
                    'light_warning': alert_dist.get(1, 0) / total * 100,
                    'moderate_warning': alert_dist.get(2, 0) / total * 100,
                    'severe_warning': alert_dist.get(3, 0) / total * 100
                },
                'avg_score': method_data['alert_score'].mean()
            }
            
        return summary