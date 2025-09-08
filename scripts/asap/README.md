# ASAP Threshold Analysis Documentation

## Overview

The ASAP threshold analysis pipeline calculates monthly population exposure to different warning levels across countries in the ROSEA (Regional Office for Southern and Eastern Africa) region. The main output is `monthly_country_exposure.csv`, which contains country-level monthly aggregations of population exposure to ASAP warning thresholds.

## Analysis Logic

### Core Implementation

The analysis is implemented in the `ThresholdAnalyzer` class located at `src/asap/threshold_analyzer.py:28`. The main calculation method is `calculate_monthly_exposure()` at line 192.

### Input Data Sources

1. **Warnings Data** (`warnings_l2_ts.csv`)
   - ASAP warning levels by admin2 area and date
   - Contains crop growth warnings at administrative level 2
   - Uses semicolon separator (`;`)

2. **Population Data** (`worldpop2020_unAdj_zonal_sum_ROSEA.csv`)
   - Admin2-level population from WorldPop 2020
   - Provides population counts for each administrative unit
   - Uses comma separator (`,`)

### Warning Level Hierarchy

The analysis uses a numeric hierarchy for warning levels (defined in `src/asap/config.py:38-46`):

- **No warning**: 0
- **Warning group 1**: 1 (Watch)
- **Warning group 2**: 2 (Advisory) 
- **Warning group 3**: 3 (Alert)
- **Warning group 4**: 4 (Emergency)
- **Off season**: -1 (excluded from threshold calculations)
- **No crop/rangeland**: -2 (excluded from threshold calculations)

### Processing Pipeline

#### 1. Data Preparation (lines 114-125)
- Converts date columns to datetime format
- Creates `year_month` periods for monthly aggregation
- Maps warning level text to numeric hierarchy values

#### 2. Data Merging (lines 201-207)
- Merges warnings data with population data on `asap2_id` (admin2 identifier)
- Filters out records with missing population data
- Logs any missing population records

#### 3. Monthly Aggregation (lines 215-218)
Groups data by:
- Country (`asap0_name`)
- Year-month period
- Warning level (text and numeric)
- Sums population for each group

#### 4. Threshold Calculations (lines 220-249)

For each country-month combination, the analysis calculates:

**Individual Warning Levels:**
- Population at each specific warning level (e.g., `pop_warning_group_1`)

**Cumulative Thresholds:**
- Population at warning level X or higher (e.g., `pop_warning_2_plus`)
- Excludes off-season and no-crop areas from threshold calculations

**Key Calculation Logic:**
```python
# Calculate threshold-based populations (X+ warning levels)
for threshold in WARNING_THRESHOLDS:
    threshold_pop = group[
        (group['warning_level_numeric'] >= threshold) & 
        (group['warning_level_numeric'] >= 0)  # Exclude off-season/no-crop
    ]['population'].sum()
    
    result[f'pop_warning_{threshold}_plus'] = threshold_pop
    result[f'pct_warning_{threshold}_plus'] = (
        threshold_pop / result['total_population'] * 100 
        if result['total_population'] > 0 else 0
    )
```

**Percentage Calculations:**
- Percentage of total population at each threshold level

### Output Schema

The `monthly_country_exposure.csv` file contains the following columns:

- `country`: Country name
- `year_month`: Year-month period (YYYY-MM format)
- `total_population`: Total population for the country-month
- `pop_no_crop/rangeland`: Population in areas with no crop/rangeland
- `pop_no_warning`: Population with no warning
- `pop_warning_group_1`: Population at Warning Group 1
- `pop_warning_group_2`: Population at Warning Group 2  
- `pop_warning_group_3`: Population at Warning Group 3
- `pop_warning_group_4`: Population at Warning Group 4
- `pop_warning_1_plus`: Population at Warning Group 1 or higher
- `pct_warning_1_plus`: Percentage at Warning Group 1 or higher
- `pop_warning_2_plus`: Population at Warning Group 2 or higher
- `pct_warning_2_plus`: Percentage at Warning Group 2 or higher
- `pop_warning_3_plus`: Population at Warning Group 3 or higher
- `pct_warning_3_plus`: Percentage at Warning Group 3 or higher
- `pop_warning_4_plus`: Population at Warning Group 4 or higher
- `pct_warning_4_plus`: Percentage at Warning Group 4 or higher
- `pop_off_season`: Population in off-season areas

### Target Countries

Analysis covers 15 countries in the ROSEA region (defined in `src/asap/config.py:22-26`):
- Burundi, Comoros, Djibouti, Kenya, Malawi, Rwanda, Uganda, Tanzania, Zambia, Zimbabwe
- Angola, Eswatini, Lesotho, Madagascar, Namibia

### Key Features

1. **Cumulative Exposure Metrics**: "Warning 2+" includes all areas at Warning Group 2, 3, or 4, measuring population under Advisory level conditions or worse.

2. **Seasonal Exclusions**: Off-season and no-crop areas are excluded from threshold calculations but tracked separately.

3. **Population-Weighted**: All calculations are population-weighted, providing exposure in terms of people affected.

4. **Monthly Temporal Resolution**: Provides monthly time series for trend analysis.

## Usage

### Running the Analysis

```python
# Main entry point
python main_asap.py

# Or run the analyzer directly
from src.asap.threshold_analyzer import ThresholdAnalyzer

analyzer = ThresholdAnalyzer(use_random_population=False)
monthly_exposure = analyzer.run_full_analysis()
```

### Configuration Options

- **Real vs Test Data**: Set `use_random_population=False` for real WorldPop data, `True` for generated test data
- **Azure Blob Storage**: Configure `USE_BLOB_STORAGE` in `azure_config.py` for cloud data sources
- **Output Location**: Results saved to `data/processed/asap/threshold_analysis/monthly_country_exposure.csv`

### Testing

The analyzer includes a test mode with reproducible random population data:
- Seed: 42 (for reproducibility)
- Population range: 1,000 - 100,000 per admin2 unit
- Saves test populations to `admin2_test_populations.csv`

## Summary Statistics

The analysis provides summary statistics including:
- Total country-months analyzed
- Number of unique countries and time periods  
- Average monthly population exposure at each threshold
- Top countries by exposure percentage

Example output shows countries like Angola with significant population exposure to higher warning levels, with both absolute population counts and percentage exposure metrics.