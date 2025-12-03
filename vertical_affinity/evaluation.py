"""
Model evaluation functions.
"""
import pandas as pd
import numpy as np
from vertical_affinity.config import R4M_DATE_SQL, CURRENT_DATE_SQL, TARGET_VERTICALS


def load_validation_data(engine):
    """
    Load validation data (actual purchase behavior) from MySQL.
    
    Args:
        engine: SQLAlchemy engine for MySQL connection
        
    Returns:
        pd.DataFrame: Validation data with purchase counts and sales shares
    """
    query = f'''
SELECT
    member_uid,
    sum(case when vertical = 'Performance All Day' or vertical = 'PAD' then 1 else 0 end) as allday_purchase,
    sum(case when vertical = 'Performance Running' or vertical = 'PR' then 1 else 0 end) as running_purchase,
    sum(case when vertical = 'Performance Training ' or vertical = 'PTR' then 1 else 0 end) as training_purchase,
    sum(case when vertical = 'Performance Tennis' or vertical = 'PT' then 1 else 0 end) as tennis_purchase,
    sum(case when vertical = 'Performance Outdoor' or vertical = 'PO' then 1 else 0 end) as outdoor_purchase,
    sum(case when vertical = 'Performance All Day' or vertical = 'PAD' then `BR Revenue` end)/sum(`BR Revenue`) as allday_sales_share,
    sum(case when vertical = 'Performance Running' or vertical = 'PR' then `BR Revenue` end)/sum(`BR Revenue`) as running_sales_share,
    sum(case when vertical = 'Performance Training ' or vertical = 'PTR' then `BR Revenue` end)/sum(`BR Revenue`) as training_sales_share,
    sum(case when vertical = 'Performance Tennis' or vertical = 'PT' then `BR Revenue` end)/sum(`BR Revenue`) as tennis_sales_share,
    sum(case when vertical = 'Performance Outdoor' or vertical = 'PO' then `BR Revenue` end)/sum(`BR Revenue`) as outdoor_sales_share 
FROM
    `dwd_dtc_store_sales_order_detail` t1
LEFT JOIN `dim_product_colors_info` t2
ON t1.`product_id(style)` = t2.`product_id(style)` 
where (member_uid != '' or member_uid != NULL ) and is_gift = 0  
    and t1.date >= {R4M_DATE_SQL} and t1.date < {CURRENT_DATE_SQL}
group by 1
'''
    
    try:
        validation = pd.read_sql(query, engine)
        validation = validation.fillna(0)
        return validation
    except Exception as e:
        print(f"读取数据失败: {e}")
        raise


def compare_vertical_performance(df, vertical_name):
    """
    Compare predicted vs non-predicted groups for a specific vertical.
    
    Args:
        df: Dataframe with predictions and validation data
        vertical_name: Vertical name to compare
        
    Returns:
        dict: Comparison metrics
    """
    purchase_col = f'{vertical_name}_purchase'
    sales_share_col = f'{vertical_name}_sales_share'
    
    if purchase_col not in df.columns or sales_share_col not in df.columns:
        print(f"错误：DataFrame 缺少 {purchase_col} 或 {sales_share_col} 列，跳过 {vertical_name}。")
        return None
    
    # Split groups
    group_predicted = df[df['Predicted_Vertical'] == vertical_name]
    group_not_predicted = df[df['Predicted_Vertical'] != vertical_name]
    
    # Calculate metrics
    cvr_predicted = (
        (group_predicted[purchase_col] > 0).sum() / len(group_predicted) 
        if len(group_predicted) > 0 else 0
    )
    cvr_not_predicted = (
        (group_not_predicted[purchase_col] > 0).sum() / len(group_not_predicted) 
        if len(group_not_predicted) > 0 else 0
    )
    
    avg_purchase_predicted = (
        group_predicted[purchase_col].mean() 
        if len(group_predicted) > 0 else 0
    )
    avg_purchase_not_predicted = (
        group_not_predicted[purchase_col].mean() 
        if len(group_not_predicted) > 0 else 0
    )
    
    avg_sales_share_predicted = (
        group_predicted[sales_share_col].mean() 
        if len(group_predicted) > 0 else 0
    )
    avg_sales_share_not_predicted = (
        group_not_predicted[sales_share_col].mean() 
        if len(group_not_predicted) > 0 else 0
    )
    
    results = {
        'Vertical': vertical_name,
        'Predicted_Group_Size': len(group_predicted),
        'Not_Predicted_Group_Size': len(group_not_predicted),
        'CVR_Predicted': cvr_predicted,
        'CVR_Not_Predicted': cvr_not_predicted,
        'CVR_Ratio': cvr_predicted / cvr_not_predicted if cvr_not_predicted > 0 else np.nan,
        'Avg_Purchase_Predicted': avg_purchase_predicted,
        'Avg_Purchase_Not_Predicted': avg_purchase_not_predicted,
        'Purchase_Ratio': (
            avg_purchase_predicted / avg_purchase_not_predicted 
            if avg_purchase_not_predicted > 0 else np.nan
        ),
        'Avg_Sales_Share_Predicted': avg_sales_share_predicted,
        'Avg_Sales_Share_Not_Predicted': avg_sales_share_not_predicted,
        'Sales_Share_Ratio': (
            avg_sales_share_predicted / avg_sales_share_not_predicted 
            if avg_sales_share_not_predicted > 0 else np.nan
        ),
    }
    return results


def evaluate_model(df_scored, validation_df):
    """
    Evaluate model performance across all verticals.
    
    Args:
        df_scored: Dataframe with predictions
        validation_df: Validation dataframe with actual behavior
        
    Returns:
        pd.DataFrame: Evaluation results
    """
    # Merge predictions with validation data
    affinity_score_cols = [
        f'Affinity_Score_{v}' for v in TARGET_VERTICALS
    ]
    
    val = df_scored[
        ['member_uid', 'Predicted_Vertical'] + affinity_score_cols
    ].merge(validation_df, on='member_uid', how='left')
    val = val.fillna(0)
    
    # Compare all verticals
    comparison_results = []
    for vertical in TARGET_VERTICALS:
        result = compare_vertical_performance(val, vertical)
        if result:
            comparison_results.append(result)
    
    df_comparison = pd.DataFrame(comparison_results)
    
    print("\n--- ✅ Affinity Score 预测效果验证结果 ---")
    print(df_comparison.round(4))
    
    print("\n--- 按 CVR Ratio 排序的品类 ---")
    print(df_comparison.sort_values(by='CVR_Ratio', ascending=False).round(4))
    
    return df_comparison, val

