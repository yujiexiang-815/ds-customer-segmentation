# Vertical Affinity Analysis Package

A structured Python package for customer vertical segmentation analysis.

## Code Structure

### Directory Structure

```
vertical_affinity/
├── __init__.py              # Package initialization and version info
├── config.py                # Configuration (database, constants, weights, dates)
├── database.py              # Database connection management (MySQL & Trino)
├── data_loader.py           # Data loading functions (product, member, employee tables)
├── digital_behavior.py      # Digital behavior processing (PDP, ATC, Navigation)
├── community_activity.py    # Community activity data processing
├── rfm_calculator.py        # RFM metrics calculation by vertical
├── feature_engineering.py  # Feature merging, imputation, normalization
├── scoring.py               # Affinity score calculation and vertical assignment
├── evaluation.py            # Model evaluation and validation metrics
└── main.py                  # Main pipeline entry point and orchestration
```

### Module Details

#### **config.py**
Centralized configuration file containing:
- Database connection parameters (MySQL and Trino)
- Date calculations (CURRENT_DATE, R4M_DATE, R10M_DATE)
- Vertical categories and mappings
- Feature weights for scoring
- Table names and constants

**Key Constants:**
- `REQUIRED_VERTICALS`: ['running', 'training', 'outdoor', 'allday', 'tennis']
- `DIMENSION_WEIGHTS_TEMPLATE`: Feature weights for affinity scoring
- `VERTICAL_MAPPING`: Mapping from product vertical names to standardized names

#### **database.py**
Database connection management:
- `create_mysql_engine()`: Creates SQLAlchemy engine for MySQL
- `create_trino_connection()`: Creates Trino DBAPI connection

#### **data_loader.py**
Data loading from MySQL:
- `load_product_table()`: Loads product/vertical mapping table
- `load_employee_table()`: Loads employee table
- `load_member_table()`: Loads member table
- `filter_employees()`: Removes employees from member list

#### **digital_behavior.py**
Processes digital behavior data from Trino:
- `load_tracking_data()`: Loads event tracking data (PDP views, ATC, navigation)
- `merge_product_vertical()`: Merges events with product vertical information
- `calculate_affinity_metrics()`: Calculates 6M counts and days since last for events
- `process_pdp_atc()`: Processes PDP views and Add to Cart into wide format
- `load_navigation_data()`: Loads category navigation click data
- `merge_digital_features()`: Combines all digital behavior features

**Output:** DataFrame with columns like `PDP_View_6M_Count_running`, `ATC_Days_Since_Last_tennis`, `navi_6M_count_outdoor`, etc.

#### **community_activity.py**
Processes community activity participation:
- `load_activity_data()`: Loads community activity data from MySQL
- `categorize_activities()`: Categorizes activities by vertical using name patterns
- `process_activity_features()`: Converts to wide format with counts per vertical

**Output:** DataFrame with columns like `activity_count_running`, `activity_count_tennis`, etc.

#### **rfm_calculator.py**
Calculates RFM (Recency, Frequency, Monetary) metrics by vertical:
- `calculate_rfm_by_vertical()`: Executes SQL query to calculate RFM metrics
- `process_rfm_features()`: Processes RFM data (clips negatives, fills missing recency)

**Output:** DataFrame with columns like `R_running`, `F_4m_tennis`, `M_1y_outdoor`, etc.

#### **feature_engineering.py**
Feature engineering and preprocessing:
- `merge_all_features()`: Merges digital, activity, and RFM features
- `filter_members_with_touchpoints()`: Removes members without any touchpoints
- `impute_missing_values()`: Fills missing values (9999 for recency, 0 for counts)
- `normalize_features()`: Normalizes features using percentile ranking

**Normalization Logic:**
- Recency features: Inverted percentile (smaller = better score)
- Positive features: Direct percentile (larger = better score)

#### **scoring.py**
Calculates affinity scores and assigns predicted verticals:
- `calculate_affinity_score()`: Calculates weighted affinity score for a vertical
- `assign_predicted_vertical()`: Assigns predicted vertical based on max affinity score

**Scoring Formula:** Weighted sum of normalized feature scores

#### **evaluation.py**
Model evaluation and validation:
- `load_validation_data()`: Loads actual purchase behavior for validation
- `compare_vertical_performance()`: Compares predicted vs non-predicted groups
- `evaluate_model()`: Evaluates model across all verticals

**Metrics:** CVR Ratio, Purchase Ratio, Sales Share Ratio

#### **monitoring.py**
Feature distribution monitoring and drift detection:
- `run_feature_distribution_checks()`: Logs key statistics each pipeline run
  - Total members
  - Touchpoint coverage (count and percentage)
  - Distribution of valid feature counts
  - `all_features_df.describe()` summary
  - `df_normalized.describe()` summary
  - Predicted vertical distribution (count & %)

#### **main.py**
Main pipeline orchestration:
- `main()`: Executes the complete pipeline end-to-end

**Pipeline Flow:**
1. Setup database connections
2. Load base tables (product, member, employee)
3. Process digital behavior data
4. Process community activity data
5. Calculate RFM metrics
6. Merge all features
7. Filter members with touchpoints
8. Engineer features (imputation & normalization)
9. Calculate scores and assign verticals
10. Evaluate model performance

### Data Flow

```
MySQL/Trino Databases
    ↓
[data_loader.py] → Base Tables (product, member, employee)
    ↓
[digital_behavior.py] → Digital Features (PDP, ATC, Navigation)
[community_activity.py] → Activity Features
[rfm_calculator.py] → RFM Features
    ↓
[feature_engineering.py] → Merged & Normalized Features
    ↓
[scoring.py] → Affinity Scores & Predicted Verticals
    ↓
[evaluation.py] → Model Evaluation Metrics
```

### Module Dependencies

```
main.py
├── database.py
├── data_loader.py
│   └── config.py
├── digital_behavior.py
│   └── config.py
├── community_activity.py
│   └── config.py
├── rfm_calculator.py
│   └── config.py
├── feature_engineering.py
├── scoring.py
│   └── config.py
└── evaluation.py
    └── config.py
```

## Usage

### Run the complete pipeline:

```python
from vertical_affinity.main import main

scored_df, comparison_df = main()
```

### Run individual components:

```python
from vertical_affinity.database import create_mysql_engine, create_trino_connection
from vertical_affinity.data_loader import load_product_table
from vertical_affinity.digital_behavior import load_tracking_data, process_pdp_atc

# Setup connections
engine = create_mysql_engine()
trino_conn = create_trino_connection()

# Load and process data
product_df = load_product_table(engine)
event_df = load_tracking_data(trino_conn)
# ... etc
```

## Configuration

Edit `config.py` to modify:
- Database connection settings
- Date ranges
- Vertical categories
- Feature weights
- Table names

## Dependencies

Install required packages:

```bash
pip install -r requirements.txt
```

## Output

The pipeline produces:
1. **scored_df**: DataFrame with affinity scores and predicted verticals for each member
2. **comparison_df**: Evaluation metrics comparing predicted vs non-predicted groups

