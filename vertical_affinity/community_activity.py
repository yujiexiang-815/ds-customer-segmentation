"""
Community activity data processing.
"""
import pandas as pd
import numpy as np
from vertical_affinity.config import REQUIRED_VERTICALS


def load_activity_data(engine):
    """
    Load community activity data from MySQL.
    
    Args:
        engine: SQLAlchemy engine for MySQL connection
        
    Returns:
        pd.DataFrame: Activity data
    """
    query = '''SELECT t1.*,t2.name,t2.activity_label
    FROM `dwd_community_activity` t1
    join `dim_community_info` t2
    on t1.community_activity_id = t2.id
    '''
    
    try:
        activity = pd.read_sql(query, engine)
        print("\n--- 数据库数据加载到 Pandas DataFrame ---")
        return activity
    except Exception as e:
        print(f"读取数据失败: {e}")
        raise


def categorize_activities(activity_df):
    """
    Categorize activities by vertical based on activity name patterns.
    
    Args:
        activity_df: Activity dataframe
        
    Returns:
        pd.DataFrame: Activity dataframe with activity_category column
    """
    cond_run = activity_df['name'].str.contains(
        '跑|lsd|间歇|变速跑|例跑|夜跑|公里|km|shake out run|run now|畅跑|跑坡',
        na=False, case=False
    )
    
    cond_outdoor = activity_df['name'].str.contains(
        '徒步|山|龙洞',
        na=False, case=False
    )
    
    cond_train = activity_df['name'].str.contains(
        '训练|力量|康复|crossfit|training|hyrox|备赛|分享会|夏训|课程',
        na=False, case=False
    )
    
    cond_tennis = activity_df['name'].str.contains(
        '网球',
        na=False, case=False
    )
    
    conditions = [cond_run, cond_outdoor, cond_train, cond_tennis]
    choices = ['running', 'outdoor', 'training', 'tennis']
    default_value = 'allday'
    
    activity_df['activity_category'] = np.select(
        conditions, choices, default=default_value
    )
    
    return activity_df


def process_activity_features(activity_df):
    """
    Process activity data into wide format features.
    
    Args:
        activity_df: Activity dataframe with activity_category
        
    Returns:
        pd.DataFrame: Wide format activity features
    """
    # Count activities by member and category
    result = activity_df.dropna(subset=['activity_category']).groupby(
        ['member_uid', 'activity_category']
    ).size().reset_index(name='count')
    
    # Optimized: Use pivot_table instead of manual pivot operations
    # This is more efficient and handles missing combinations automatically
    final_activity = result.pivot_table(
        index='member_uid',
        columns='activity_category',
        values='count',
        fill_value=0
    ).reset_index()
    
    # Rename columns to match expected format
    final_activity.columns = [
        'member_uid' if col == 'member_uid' else f'activity_count_{col}'
        for col in final_activity.columns
    ]
    
    # Ensure all required verticals are present as columns
    for vertical in REQUIRED_VERTICALS:
        col_name = f'activity_count_{vertical}'
        if col_name not in final_activity.columns:
            final_activity[col_name] = 0
    
    expected_cols = 1 + len(REQUIRED_VERTICALS)
    print(f"✅ 最终列数检查: {len(final_activity.columns)} (预期: {expected_cols})")
    
    return final_activity

