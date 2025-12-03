### 📊 Project Overview

This project is dedicated to segmenting our large customer base into meaningful and actionable groups by analyzing their behavior and value metrics. The primary goal is to provide data-driven insights that inform and optimize strategies across marketing, product development, and resource allocation.

### 🚀 Key Objectives

  * **Identify High-Value Segments:** Discover and profile the customer groups that contribute the most significant revenue to the business.
  * **Improve Marketing Efficiency:** Enable personalized and targeted campaigns, leading to higher response rates and a better Return on Investment (ROI).
  * **Guide Product Strategy:** Inform the product roadmap and feature development based on the specific needs of different customer segments.

### 🛠️ Methodology

The project utilizes the following key analytical and modeling steps:

  * **Data Sources:** Order history, browsing logs, and demographic information (where available).
  * **Feature Engineering:** Construction of **RFM (Recency, Frequency, Monetary)** features and key behavioral preference metrics.
  * **Clustering Analysis:** Primary focus on **K-Means** or **Hierarchical Clustering** algorithms to identify naturally occurring groups.
  * **Segmentation Profiling:** Qualitative and quantitative description of each cluster to create actionable business personas.

### 📁 Repository Structure

```
.
├── vertical_affinity/          # Vertical Affinity Analysis Package
│   ├── __init__.py            # Package initialization
│   ├── config.py              # Configuration (database, constants, weights)
│   ├── database.py            # Database connection management
│   ├── data_loader.py         # Data loading functions
│   ├── digital_behavior.py    # Digital behavior processing (PDP, ATC, Navigation)
│   ├── community_activity.py  # Community activity processing
│   ├── rfm_calculator.py      # RFM metrics calculation by vertical
│   ├── feature_engineering.py # Feature merging, imputation, normalization
│   ├── scoring.py             # Affinity score calculation and vertical assignment
│   ├── evaluation.py          # Model evaluation and validation
│   ├── main.py                # Main pipeline entry point
│   └── README.md              # Package documentation
├── RFM.ipynb                   # RFM analysis notebook
├── vertical affinity.ipynb    # Original vertical affinity analysis notebook
├── README.md                   # This file
└── requirements.txt           # Project dependencies
```

### 📈 Expected Output

  * **Final Segmentation Model:** Model file (e.g., K-Means model) ready for production deployment.
  * **Segment Profile Report:** Detailed reports on each segment (average RFM scores, purchasing preferences, demographics).
  * **Customer Tags:** Customer IDs with their corresponding segment labels, ready for ingestion by BI or CRM systems.

### 🏗️ Code Structure

#### **Vertical Affinity Package**

The `vertical_affinity/` package provides a structured implementation for customer vertical segmentation analysis. It processes digital behavior, community activity, and RFM metrics to assign customers to vertical categories (running, training, outdoor, tennis, allday).

**Key Components:**

1. **Configuration (`config.py`)**
   - Database connection settings (MySQL & Trino)
   - Date calculations and time windows
   - Vertical categories and mappings
   - Feature weights for scoring

2. **Data Loading (`data_loader.py`, `database.py`)**
   - MySQL and Trino connection management
   - Product, member, and employee table loading
   - Employee filtering

3. **Feature Extraction**
   - **Digital Behavior (`digital_behavior.py`)**: Processes PDP views, Add to Cart events, and navigation clicks
   - **Community Activity (`community_activity.py`)**: Analyzes community participation by vertical
   - **RFM Calculator (`rfm_calculator.py`)**: Calculates Recency, Frequency, Monetary metrics by vertical

4. **Feature Engineering (`feature_engineering.py`)**
   - Merges all feature sources
   - Imputes missing values
   - Normalizes features using percentile ranking

5. **Scoring (`scoring.py`)**
   - Calculates weighted affinity scores for each vertical
   - Assigns predicted vertical based on maximum affinity score

6. **Evaluation (`evaluation.py`)**
   - Validates predictions against actual purchase behavior
   - Calculates CVR ratios, purchase ratios, and sales share ratios

**Usage:**

```python
from vertical_affinity.main import main

# Run complete pipeline
scored_df, comparison_df = main()
```

The pipeline produces:
- **scored_df**: DataFrame with affinity scores and predicted verticals for each member
- **comparison_df**: Evaluation metrics comparing predicted vs non-predicted groups

For detailed documentation, see `vertical_affinity/README.md`.

-----
