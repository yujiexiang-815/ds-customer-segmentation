"""
Affinity score calculation and vertical assignment.
"""
import pandas as pd
from vertical_affinity.config import (
    TARGET_VERTICALS,
    DIMENSION_WEIGHTS_TEMPLATE
)


def calculate_affinity_score(df, vertical_name, abstract_weights):
    """
    Calculate affinity score for a specific vertical.
    
    Args:
        df: Dataframe with normalized score features
        vertical_name: Vertical name (e.g., 'running', 'tennis')
        abstract_weights: Dictionary of abstract feature weights
        
    Returns:
        pd.DataFrame: Dataframe with added affinity score column
    """
    score_column_name = f'Affinity_Score_{vertical_name}'
    current_weights = {}
    
    # Build actual feature score column names
    for abstract_feature_key, weight in abstract_weights.items():
        new_feature_score_name = f'{abstract_feature_key}_{vertical_name}_score'
        
        if new_feature_score_name in df.columns:
            current_weights[new_feature_score_name] = weight
        else:
            print(f"⚠️ 警告：品类 {vertical_name} 缺少特征列: {new_feature_score_name}，已跳过。")
    
    # Optimized: Vectorized weighted sum calculation
    if current_weights:
        # Create a matrix of feature columns and multiply by weights, then sum
        feature_matrix = df[list(current_weights.keys())]
        weights_array = pd.Series(current_weights)
        df[score_column_name] = (feature_matrix * weights_array).sum(axis=1)
    else:
        df[score_column_name] = 0.0
    
    return df


def assign_predicted_vertical(df):
    """
    Assign predicted vertical based on highest affinity score.
    
    Args:
        df: Dataframe with affinity scores for all verticals
        
    Returns:
        pd.DataFrame: Dataframe with predicted vertical assignment
    """
    df_scored = df.copy()
    
    # Calculate scores for all verticals
    scored_columns = []
    for v_name in TARGET_VERTICALS:
        df_scored = calculate_affinity_score(
            df_scored, v_name, DIMENSION_WEIGHTS_TEMPLATE
        )
        score_col = f'Affinity_Score_{v_name}'
        scored_columns.append(score_col)
    
    # Find max affinity score
    df_scored['Max_Affinity_Score'] = df_scored[scored_columns].max(axis=1)
    
    # Assign predicted vertical
    df_scored['Predicted_Vertical_Score_Col'] = df_scored[scored_columns].idxmax(axis=1)
    
    # Handle zero scores
    df_scored.loc[
        df_scored['Max_Affinity_Score'] == 0, 
        'Predicted_Vertical_Score_Col'
    ] = 'Affinity_Score_No_Interest'
    
    # Clean vertical name
    df_scored['Predicted_Vertical'] = df_scored['Predicted_Vertical_Score_Col'].str.replace(
        'Affinity_Score_', ''
    )
    
    return df_scored, scored_columns

