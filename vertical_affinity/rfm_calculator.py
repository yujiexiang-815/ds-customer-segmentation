"""
RFM (Recency, Frequency, Monetary) metrics calculation by vertical.
"""
import pandas as pd
from vertical_affinity.config import R4M_DATE_SQL, CURRENT_DATE_SQL


def calculate_rfm_by_vertical(engine):
    """
    Calculate RFM metrics by vertical from MySQL database.
    
    Args:
        engine: SQLAlchemy engine for MySQL connection
        
    Returns:
        pd.DataFrame: RFM features by vertical
    """
    query = f'''-- 1. 定义计算基准日期 (TODAY) 和时间窗口
WITH
  params AS (
    SELECT
      CAST({R4M_DATE_SQL} AS DATE) AS TODAY,
      DATE_SUB(CAST({R4M_DATE_SQL} AS DATE), INTERVAL 4 MONTH) AS DATE_4M_AGO,
      DATE_SUB(CAST({R4M_DATE_SQL} AS DATE), INTERVAL 1 YEAR) AS DATE_1Y_AGO
  ),
  
-- 2. 筛选相关品类并标准化数据
filtered_orders AS (
  SELECT
    member_uid,
    CAST(date AS DATE) AS purchase_date,
    case when vertical = 'Performance All Day' or vertical = 'PAD' then 'allday'
      when vertical = 'Performance Running' or vertical = 'PR' then 'running'
  when vertical = 'Performance Training ' or vertical = 'PTR' then 'training'
  when vertical = 'Performance Tennis' or vertical = 'PT' then 'tennis'
  when vertical = 'Performance Outdoor' or vertical = 'PO' then 'outdoor' end as vertical,
    `AR Revenue` as purchase_amount
  FROM
    `dwd_dtc_store_sales_order_detail` t1
  LEFT JOIN `dim_product_colors_info` t2
  ON t1.`product_id(style)` = t2.`product_id(style)` 
  where (member_uid != '' or member_uid != NULL ) and is_gift = 0 
),

-- 3. 计算 R (Recency) - 距今最近一次购买的天数
R_features AS (
  SELECT
    t1.member_uid,
    t1.vertical,
    DATEDIFF(p.TODAY,MAX(t1.purchase_date)) AS recency_days 
  FROM
    filtered_orders AS t1
  CROSS JOIN 
    params AS p
  GROUP BY
    t1.member_uid, t1.vertical, p.TODAY
),

-- 4. 计算 F & M (4个月)
FM_4M_features AS (
  SELECT
    t1.member_uid,
    t1.vertical,
    COUNT(*) AS frequency_4m,
    SUM(t1.purchase_amount) AS monetary_4m
  FROM
    filtered_orders AS t1, params AS p
  WHERE
    t1.purchase_date >= p.DATE_4M_AGO
  GROUP BY
    1, 2
),

-- 5. 计算 F & M (1年)
FM_1Y_features AS (
  SELECT
    t1.member_uid,
    t1.vertical,
    COUNT(*) AS frequency_1y,
    SUM(t1.purchase_amount) AS monetary_1y
  FROM
    filtered_orders AS t1, params AS p
  WHERE
    t1.purchase_date >= p.DATE_1Y_AGO
  GROUP BY
    1, 2
)

-- 6. 最终的 Full Join 和 Pivot (使用条件聚合实现)
SELECT
  t1.member_uid,

  -- R 特征
  MAX(CASE WHEN t2.vertical = 'running' THEN t2.recency_days ELSE NULL END) AS R_running,
  MAX(CASE WHEN t2.vertical = 'tennis' THEN t2.recency_days ELSE NULL END) AS R_tennis,
  MAX(CASE WHEN t2.vertical = 'allday' THEN t2.recency_days ELSE NULL END) AS R_allday,
  MAX(CASE WHEN t2.vertical = 'training' THEN t2.recency_days ELSE NULL END) AS R_training,
  MAX(CASE WHEN t2.vertical = 'outdoor' THEN t2.recency_days ELSE NULL END) AS R_outdoor,

  -- F_4M 特征
  COALESCE(MAX(CASE WHEN t3.vertical = 'running' THEN t3.frequency_4m ELSE NULL END), 0) AS F_4m_running,
  COALESCE(MAX(CASE WHEN t3.vertical = 'tennis' THEN t3.frequency_4m ELSE NULL END), 0) AS F_4m_tennis,
  COALESCE(MAX(CASE WHEN t3.vertical = 'allday' THEN t3.frequency_4m ELSE NULL END), 0) AS F_4m_allday,
  COALESCE(MAX(CASE WHEN t3.vertical = 'training' THEN t3.frequency_4m ELSE NULL END), 0) AS F_4m_training,
  COALESCE(MAX(CASE WHEN t3.vertical = 'outdoor' THEN t3.frequency_4m ELSE NULL END), 0) AS F_4m_outdoor,

  -- M_4M 特征
  COALESCE(MAX(CASE WHEN t3.vertical = 'running' THEN t3.monetary_4m ELSE NULL END), 0) AS M_4m_running,
  COALESCE(MAX(CASE WHEN t3.vertical = 'tennis' THEN t3.monetary_4m ELSE NULL END), 0) AS M_4m_tennis,
  COALESCE(MAX(CASE WHEN t3.vertical = 'allday' THEN t3.monetary_4m ELSE NULL END), 0) AS M_4m_allday,
  COALESCE(MAX(CASE WHEN t3.vertical = 'training' THEN t3.monetary_4m ELSE NULL END), 0) AS M_4m_training,
  COALESCE(MAX(CASE WHEN t3.vertical = 'outdoor' THEN t3.monetary_4m ELSE NULL END), 0) AS M_4m_outdoor,

  -- F_1Y 特征
  COALESCE(MAX(CASE WHEN t4.vertical = 'running' THEN t4.frequency_1y ELSE NULL END), 0) AS F_1y_running,
  COALESCE(MAX(CASE WHEN t4.vertical = 'tennis' THEN t4.frequency_1y ELSE NULL END), 0) AS F_1y_tennis,
  COALESCE(MAX(CASE WHEN t4.vertical = 'allday' THEN t4.frequency_1y ELSE NULL END), 0) AS F_1y_allday,
  COALESCE(MAX(CASE WHEN t4.vertical = 'training' THEN t4.frequency_1y ELSE NULL END), 0) AS F_1y_training,
  COALESCE(MAX(CASE WHEN t4.vertical = 'outdoor' THEN t4.frequency_1y ELSE NULL END), 0) AS F_1y_outdoor,

  -- M_1Y 特征
  COALESCE(MAX(CASE WHEN t4.vertical = 'running' THEN t4.monetary_1y ELSE NULL END), 0) AS M_1y_running,
  COALESCE(MAX(CASE WHEN t4.vertical = 'tennis' THEN t4.monetary_1y ELSE NULL END), 0) AS M_1y_tennis,
  COALESCE(MAX(CASE WHEN t4.vertical = 'allday' THEN t4.monetary_1y ELSE NULL END), 0) AS M_1y_allday,
  COALESCE(MAX(CASE WHEN t4.vertical = 'training' THEN t4.monetary_1y ELSE NULL END), 0) AS M_1y_training,
  COALESCE(MAX(CASE WHEN t4.vertical = 'outdoor' THEN t4.monetary_1y ELSE NULL END), 0) AS M_1y_outdoor

FROM
  -- 获取所有用户 ID
  (SELECT DISTINCT member_uid FROM `dwd_dtc_store_sales_order_detail` 
  where (member_uid != '' or member_uid != NULL )) AS t1

  -- 左连接 R 特征
  LEFT JOIN R_features AS t2 ON t1.member_uid = t2.member_uid
  -- 左连接 FM 4M 特征
  LEFT JOIN FM_4M_features AS t3 ON t1.member_uid = t3.member_uid AND t2.vertical = t3.vertical
  -- 左连接 FM 1Y 特征
  LEFT JOIN FM_1Y_features AS t4 ON t1.member_uid = t4.member_uid AND t2.vertical = t4.vertical

GROUP BY
  t1.member_uid
ORDER BY
  t1.member_uid
  '''
    
    try:
        base_RFM_vertical = pd.read_sql(query, engine)
        print("\n--- 数据库数据加载到 Pandas DataFrame ---")
        return base_RFM_vertical
    except Exception as e:
        print(f"读取数据失败: {e}")
        raise


def process_rfm_features(rfm_df):
    """
    Process RFM dataframe: clip negative values and fill missing recency values.
    
    Args:
        rfm_df: Raw RFM dataframe
        
    Returns:
        pd.DataFrame: Processed RFM dataframe
    """
    # Clip negative values to 0
    cols_to_clip = rfm_df.columns.drop('member_uid')
    rfm_df[cols_to_clip] = rfm_df[cols_to_clip].clip(lower=0)
    
    # Fill missing recency values with 9999
    final_RFM_vertical = rfm_df.fillna({
        'R_running': 9999,
        'R_tennis': 9999,
        'R_allday': 9999,
        'R_training': 9999,
        'R_outdoor': 9999
    })
    
    return final_RFM_vertical

