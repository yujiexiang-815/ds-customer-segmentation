"""
Feature distribution and drift monitoring utilities.
"""
import pandas as pd


def run_feature_distribution_checks(
    member_df,
    all_features_df,
    filtered_df,
    normalized_df,
    scored_df,
):
    """
    Print feature distribution summaries to help detect drift.

    Args:
        member_df: DataFrame of members after employee filtering.
        all_features_df: DataFrame after merging all features (before filtering).
        filtered_df: DataFrame after removing members without touchpoints.
        normalized_df: DataFrame after normalization.
        scored_df: Final scored DataFrame with predictions.
    """

    print("\n===== Feature Distribution Checks =====")

    # 1. Total member counts
    total_members = len(member_df)
    print(f"\n1) Total members (post employee filter): {total_members:,}")

    # 2. Members with touchpoints (count & %)
    members_with_touchpoints = len(filtered_df)
    touchpoint_pct = (
        members_with_touchpoints / total_members * 100 if total_members else 0
    )
    print(
        f"2) Members with touchpoints: {members_with_touchpoints:,} "
        f"({touchpoint_pct:.2f}%)"
    )

    # 3. Distribution of valid feature counts (replicates legacy valid_column_count)
    valid_feature_counts = all_features_df.notna().sum(axis=1)
    valid_summary = valid_feature_counts.value_counts().sort_index()
    print("\n3) Valid feature count distribution (count of non-null columns per member):")
    print(valid_summary.to_string())

    # 4. Descriptive stats for merged feature set
    print("\n4) all_features_df.describe():")
    try:
        print(all_features_df.describe().transpose())
    except Exception as exc:
        print(f"   Warning: Unable to compute describe for all_features_df ({exc}).")

    # 5. Descriptive stats for normalized features
    print("\n5) df_normalized.describe():")
    try:
        print(normalized_df.describe().transpose())
    except Exception as exc:
        print(f"   Warning: Unable to compute describe for normalized_df ({exc}).")

    # 6. Predicted vertical distribution (count & %)
    print("\n6) Predicted Vertical distribution:")
    pred_counts = scored_df["Predicted_Vertical"].value_counts(dropna=False)
    pred_pct = scored_df["Predicted_Vertical"].value_counts(
        dropna=False, normalize=True
    ) * 100
    pred_df = pd.DataFrame(
        {
            "count": pred_counts,
            "percentage": pred_pct.round(2),
        }
    )
    print(pred_df)

    print("===== End of Feature Distribution Checks =====\n")

