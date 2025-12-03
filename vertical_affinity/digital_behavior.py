"""
Digital behavior data processing: PDP views, Add to Cart, and Navigation.
"""
import pandas as pd
from vertical_affinity.config import (
    R10M_DATE_SQL,
    R4M_DATE_SQL,
    CURRENT_DATE,
    VERTICAL_MAPPING,
    REQUIRED_VERTICALS,
    EVENT_PDP_VIEW,
    EVENT_ADD_TO_CART,
    BASE_COLUMNS
)


def load_tracking_data(conn):
    """
    Load event tracking data from Trino.
    
    Args:
        conn: Trino database connection
        
    Returns:
        pd.DataFrame: Event tracking data
    """
    query = f'''SELECT 
"#event_name" as event_name,
"#event_time" as event_time,
"#account_id" as account_id,
"page_type",
"#os" as os,
"login_pt",
"#mp_platform" as mp_platform,
"current_status",
"product_name",
"product_type",
"search_term",
"product_id"
FROM v_event_3
where "#account_id" is not null 
and "#event_time" > date({R10M_DATE_SQL}) and "#event_time" < date({R4M_DATE_SQL})
and "#event_name" in ('add_to_cart','pdp_view','plp_category_navi_click','search')
'''
    event = pd.read_sql(query, conn)
    print(f"Loaded {len(event)} event records")
    return event


def merge_product_vertical(event_df, product_df):
    """
    Merge event data with product vertical information.
    
    Args:
        event_df: Event tracking dataframe
        product_df: Product dataframe with vertical information
        
    Returns:
        pd.DataFrame: Merged dataframe with vertical information
    """
    event_df['event_time'] = pd.to_datetime(event_df['event_time'])
    
    # Merge with product table
    merged_df = event_df.merge(
        product_df[['model', 'vertical']],
        left_on='product_name',
        right_on='model',
        how='left'
    )
    
    # Apply vertical mapping
    merged_df['vertical'] = merged_df['vertical'].replace(VERTICAL_MAPPING)
    
    # Filter to required verticals
    merged_df = merged_df[merged_df['vertical'].isin(REQUIRED_VERTICALS)]
    
    # Drop rows with missing vertical
    if merged_df['vertical'].isnull().any():
        print("⚠️ 警告: 存在未能匹配到产品类别的记录。")
        merged_df.dropna(subset=['vertical'], inplace=True)
    
    return merged_df


def calculate_affinity_metrics(df, event_name, prefix):
    """
    Calculate affinity metrics for a specific event type.
    
    Args:
        df: Merged event dataframe
        event_name: Event name to filter (e.g., 'pdp_view', 'add_to_cart')
        prefix: Prefix for column names (e.g., 'PDP_View_', 'ATC_')
        
    Returns:
        pd.DataFrame: Metrics dataframe with counts and days since last
    """
    event_df = df[df['event_name'] == event_name].copy()
    if event_df.empty:
        return None
    
    # Optimized: Calculate both count and max time in a single groupby operation
    grouped = event_df.groupby(BASE_COLUMNS).agg({
        'event_time': ['count', 'max']
    }).reset_index()
    
    # Flatten column names
    grouped.columns = BASE_COLUMNS + [f'{prefix}6M_Count', 'Latest_Action_Time']
    
    # Calculate days since last
    grouped[f'{prefix}Days_Since_Last'] = (
        CURRENT_DATE - grouped['Latest_Action_Time']
    ).dt.days
    
    # Drop intermediate column
    final_metrics = grouped.drop(columns=['Latest_Action_Time'])
    
    return final_metrics


def process_pdp_atc(merged_df):
    """
    Process PDP view and Add to Cart metrics.
    
    Args:
        merged_df: Merged event dataframe with vertical information
        
    Returns:
        pd.DataFrame: Wide format dataframe with PDP and ATC metrics
    """
    # Calculate metrics for PDP and ATC
    pdp_result = calculate_affinity_metrics(merged_df, EVENT_PDP_VIEW, 'PDP_View_')
    atc_result = calculate_affinity_metrics(merged_df, EVENT_ADD_TO_CART, 'ATC_')
    
    # Merge intermediate results
    intermediate_result = pd.merge(
        pdp_result,
        atc_result,
        on=BASE_COLUMNS,
        how='outer'
    )
    
    # Fill missing values
    intermediate_result = intermediate_result.fillna({
        'PDP_View_6M_Count': 0,
        'PDP_View_Days_Since_Last': 9999,
        'ATC_6M_Count': 0,
        'ATC_Days_Since_Last': 9999
    })
    
    # Create complete base table with all combinations
    all_account_ids = merged_df['account_id'].unique()
    all_combinations_df = pd.MultiIndex.from_product(
        [all_account_ids, REQUIRED_VERTICALS],
        names=BASE_COLUMNS
    ).to_frame(index=False)
    
    base_result = pd.merge(
        all_combinations_df,
        intermediate_result,
        on=BASE_COLUMNS,
        how='left'
    )
    
    # Fill missing values again
    base_result = base_result.fillna({
        'PDP_View_6M_Count': 0,
        'PDP_View_Days_Since_Last': 9999,
        'ATC_6M_Count': 0,
        'ATC_Days_Since_Last': 9999
    })
    
    # Pivot to wide format
    df_pivot = base_result.set_index(BASE_COLUMNS)
    pdp_atc = df_pivot.unstack(level='vertical')
    
    # Flatten column names
    pdp_atc.columns = [
        f'{col[0]}_{col[1]}' 
        for col in pdp_atc.columns
    ]
    
    pdp_atc = pdp_atc.reset_index()
    
    expected_cols = 1 + 4 * len(REQUIRED_VERTICALS)
    print(f"✅ 最终列数检查: {len(pdp_atc.columns)} (预期: {expected_cols})")
    
    return pdp_atc


def load_navigation_data(conn):
    """
    Load navigation category click data from Trino.
    
    Args:
        conn: Trino database connection
        
    Returns:
        pd.DataFrame: Navigation data with counts by vertical
    """
    query = f'''select "#account_id" as account_id, 
sum(case when cat_name = '路跑' then 1 else 0 end) as navi_6M_count_running,
sum(case when cat_name = '运动生活' then 1 else 0 end) as navi_6M_count_allday,
sum(case when cat_name = '徒步越野' then 1 else 0 end) as navi_6M_count_outdoor,
sum(case when cat_name = '网球' then 1 else 0 end) as navi_6M_count_tennis,
sum(case when cat_name = '综训' then 1 else 0 end) as navi_6M_count_training
FROM v_event_3 
where "#account_id" is not null 
and "#event_time" > date({R10M_DATE_SQL}) and "#event_time" < date({R4M_DATE_SQL})
and "#event_name" in ('plp_category_navi_click')
and cat_level > 2
group by 1
'''
    navi_df = pd.read_sql(query, conn)
    return navi_df


def merge_digital_features(pdp_atc_df, navi_df):
    """
    Merge PDP/ATC features with navigation features.
    
    Args:
        pdp_atc_df: PDP and ATC features dataframe
        navi_df: Navigation features dataframe
        
    Returns:
        pd.DataFrame: Combined digital behavior features
    """
    final_digital = pdp_atc_df.merge(navi_df, on='account_id', how='outer')
    return final_digital

