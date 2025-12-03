"""
Main entry point for vertical affinity analysis pipeline.
"""
import pandas as pd
from vertical_affinity.database import create_mysql_engine, create_trino_connection
from vertical_affinity.data_loader import (
    load_product_table,
    load_employee_table,
    load_member_table,
    filter_employees
)
from vertical_affinity.digital_behavior import (
    load_tracking_data,
    merge_product_vertical,
    process_pdp_atc,
    load_navigation_data,
    merge_digital_features
)
from vertical_affinity.community_activity import (
    load_activity_data,
    categorize_activities,
    process_activity_features
)
from vertical_affinity.rfm_calculator import (
    calculate_rfm_by_vertical,
    process_rfm_features
)
from vertical_affinity.feature_engineering import (
    merge_all_features,
    filter_members_with_touchpoints,
    impute_missing_values,
    normalize_features
)
from vertical_affinity.scoring import assign_predicted_vertical
from vertical_affinity.evaluation import load_validation_data, evaluate_model
from vertical_affinity.monitoring import run_feature_distribution_checks


def main():
    """
    Main pipeline execution function.
    """
    print("=" * 60)
    print("Vertical Affinity Analysis Pipeline")
    print("=" * 60)
    
    # ======================== DATABASE CONNECTIONS ========================
    print("\n[1/10] Setting up database connections...")
    engine = create_mysql_engine()
    trino_conn = create_trino_connection()
    
    # ======================== LOAD BASE TABLES ========================
    print("\n[2/10] Loading base tables...")
    product_df = load_product_table(engine)
    employee_df = load_employee_table(engine)
    member_df = load_member_table(engine)
    member_filtered = filter_employees(member_df, employee_df)
    
    # ======================== DIGITAL BEHAVIOR ========================
    print("\n[3/10] Processing digital behavior data...")
    event_df = load_tracking_data(trino_conn)
    merged_df = merge_product_vertical(event_df, product_df)
    pdp_atc_df = process_pdp_atc(merged_df)
    navi_df = load_navigation_data(trino_conn)
    digital_df = merge_digital_features(pdp_atc_df, navi_df)
    
    # ======================== COMMUNITY ACTIVITY ========================
    print("\n[4/10] Processing community activity data...")
    activity_df = load_activity_data(engine)
    activity_df = categorize_activities(activity_df)
    activity_features = process_activity_features(activity_df)
    
    # ======================== RFM BY VERTICAL ========================
    print("\n[5/10] Calculating RFM metrics by vertical...")
    rfm_df = calculate_rfm_by_vertical(engine)
    rfm_features = process_rfm_features(rfm_df)
    
    # ======================== MERGE ALL FEATURES ========================
    print("\n[6/10] Merging all features...")
    all_features_df = merge_all_features(
        member_filtered, digital_df, activity_features, rfm_features
    )
    
    # ======================== FILTER MEMBERS ========================
    print("\n[7/10] Filtering members with touchpoints...")
    filtered_df = filter_members_with_touchpoints(all_features_df)
    
    # ======================== FEATURE ENGINEERING ========================
    print("\n[8/10] Engineering features (imputation & normalization)...")
    imputed_df = impute_missing_values(filtered_df)
    normalized_df = normalize_features(imputed_df)
    
    # ======================== SCORING & ASSIGNMENT ========================
    print("\n[9/10] Calculating affinity scores and assigning verticals...")
    scored_df, scored_columns = assign_predicted_vertical(normalized_df)
    
    print("\n--- 预测品类分布 ---")
    print(scored_df['Predicted_Vertical'].value_counts())

    # ======================== FEATURE DISTRIBUTION CHECKS ========================
    print("\n[9.5/10] Running feature distribution checks...")
    run_feature_distribution_checks(
        member_filtered,
        all_features_df,
        filtered_df,
        normalized_df,
        scored_df
    )
    
    # ======================== EVALUATION ========================
    print("\n[10/10] Evaluating model performance...")
    validation_df = load_validation_data(engine)
    comparison_df, validation_merged = evaluate_model(scored_df, validation_df)
    
    # ======================== SUMMARY ========================
    print("\n" + "=" * 60)
    print("Pipeline completed successfully!")
    print("=" * 60)
    print(f"Total members analyzed: {len(scored_df)}")
    print(f"Members with predictions: {len(scored_df[scored_df['Predicted_Vertical'] != 'No_Interest'])}")
    
    return scored_df, comparison_df


if __name__ == "__main__":
    scored_df, comparison_df = main()

