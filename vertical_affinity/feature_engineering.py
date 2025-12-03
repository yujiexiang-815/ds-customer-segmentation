"""
Feature engineering: merging, imputation, and normalization.
"""
import pandas as pd
import numpy as np


def merge_all_features(member_df, digital_df, activity_df, rfm_df):
    """
    Merge all feature dataframes together.
    
    Args:
        member_df: Member dataframe
        digital_df: Digital behavior features
        activity_df: Activity features
        rfm_df: RFM features
        
    Returns:
        pd.DataFrame: Merged dataframe with all features
    """
    all_member_df = member_df[['member_uid']].merge(
        digital_df, left_on='member_uid', right_on='account_id', how='left'
    ).merge(
        activity_df, on='member_uid', how='left'
    ).merge(
        rfm_df, on='member_uid', how='left'
    )
    
    return all_member_df


def filter_members_with_touchpoints(df):
    """
    Filter out members without any touchpoints.
    
    Args:
        df: Merged dataframe
        
    Returns:
        pd.DataFrame: Filtered dataframe
    """
    # More efficient: check if any column (except member_uid) has non-null value
    # This matches original behavior: keep rows where count > 1 (i.e., more than just member_uid)
    # Exclude member_uid from the check since it's always present
    cols_to_check = [col for col in df.columns if col != 'member_uid']
    
    # Check if any column (besides member_uid) has non-null value
    # This is equivalent to count(axis=1) > 1 but much faster
    has_touchpoints = df[cols_to_check].notna().any(axis=1)
    final_df = df[has_touchpoints].copy()
    
    # Drop helper columns
    final_df.drop(columns=['account_id'], inplace=True, errors='ignore')
    
    print(f"Filtered to {len(final_df)} members with touchpoints")
    return final_df


def impute_missing_values(df):
    """
    Impute missing values in feature columns.
    
    Args:
        df: Dataframe with features
        
    Returns:
        pd.DataFrame: Dataframe with imputed values
    """
    df_imputed = df.copy()
    
    # Get all feature columns
    all_features = [
        col for col in df_imputed.columns 
        if any(keyword in col for keyword in ['R_', 'F_', 'M_', 'PDP_', 'ATC_', 'activity_'])
    ]
    
    # Separate recency columns from positive columns for batch operations
    recency_cols = [
        col for col in all_features 
        if col.startswith('R_') or col.startswith('PDP_View_Days_Since_') or col.startswith('ATC_Days_Since_')
    ]
    positive_cols = [col for col in all_features if col not in recency_cols]
    
    # Batch fillna operations (more efficient than looping)
    if recency_cols:
        df_imputed[recency_cols] = df_imputed[recency_cols].fillna(9999)
    if positive_cols:
        df_imputed[positive_cols] = df_imputed[positive_cols].fillna(0)
    
    return df_imputed


def normalize_features(df):
    """
    Normalize features using percentile ranking.
    
    Args:
        df: Dataframe with imputed features
        
    Returns:
        pd.DataFrame: Dataframe with normalized score features
    """
    df_normalized = df.copy()
    
    # Get all feature columns
    all_features = [
        col for col in df_normalized.columns 
        if any(keyword in col for keyword in ['R_', 'F_', 'M_', 'PDP_', 'ATC_', 'activity_'])
    ]
    
    # Recency columns (lower is better, so invert percentile)
    recency_cols = [
        col for col in all_features 
        if col.startswith('R_') or col.startswith('PDP_View_Days_Since_') or col.startswith('ATC_Days_Since_')
    ]
    
    # Positive columns (higher is better)
    positive_cols = [col for col in all_features if col not in recency_cols]
    
    # Batch rank operations for better performance
    if recency_cols:
        recency_ranks = df_normalized[recency_cols].rank(pct=True, method='average')
        # Invert percentile: smaller values get higher scores
        score_cols = {f'{col}_score': 1 - recency_ranks[col] for col in recency_cols}
        for col_name, score_series in score_cols.items():
            df_normalized[col_name] = score_series.fillna(0)
    
    if positive_cols:
        positive_ranks = df_normalized[positive_cols].rank(pct=True, method='average')
        # Direct percentile: larger values get higher scores
        score_cols = {f'{col}_score': positive_ranks[col] for col in positive_cols}
        for col_name, score_series in score_cols.items():
            df_normalized[col_name] = score_series.fillna(0)
    
    print("--- 特征标准化完成 ---")
    return df_normalized

