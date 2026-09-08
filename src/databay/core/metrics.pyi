from __future__ import annotations
from typing import Dict, List, Literal, Optional
from pyspark.sql import DataFrame

def compare_datasets(
    df_a: DataFrame,
    df_b: DataFrame,
    cols: List[str],
    name_a: str = "Dataset A",
    name_b: str = "Dataset B",
) -> DataFrame:
    """Compare two DataFrames and compute similarity metrics.

    Args:
        df_a: First DataFrame to compare
        df_b: Second DataFrame to compare
        cols: List of columns to include in comparison (use ["*"] for all columns)
        name_a: Display name for first dataset (default: "Dataset A")
        name_b: Display name for second dataset (default: "Dataset B")

    Returns:
        DataFrame with metric, scope, count_value and percent_value columns.
        Metrics include row counts, differences, match percentages and Jaccard similarity.

    Notes:
        Comparison uses all selected values and preserves duplicate multiplicity
        (exceptAll), rather than comparing unique sets or aligning by a key.
        Selected columns must exist in both inputs with compatible types.

    Example:
        >>> compare_datasets(source, target, ["id", "amount"]).show()
    """
    ...

def compare_columns_by_key(
    df_a: DataFrame,
    df_b: DataFrame,
    key_cols: List[str],
    compare_cols: List[str],
    show_summary_only: bool = True,
    join_type: str = "inner",
) -> DataFrame:
    """Compare specific columns between two DataFrames joined by key columns.

    Args:
        df_a: First DataFrame to compare
        df_b: Second DataFrame to compare
        key_cols: List of columns to join on
        compare_cols: List of columns to compare, or ["*"] for all non-key columns
        show_summary_only: If True, returns summary statistics per column.
                          If False, returns detailed differences (default: True)
        join_type: Join strategy for key alignment. Supported:
                  "inner", "left", "right", "full", "full_outer" (default: "inner")

    Returns:
        If show_summary_only=True:
            DataFrame with per-column match statistics

        If show_summary_only=False:
            DataFrame with detailed differences showing:
            - [key_cols]: Key columns that identify the record
            - [column]_a: Value from df_a
            - [column]_b: Value from df_b
            - [column]_match: Boolean indicating if values match
            Only rows with at least one difference are returned

    NULL handling:
        - NULL == NULL → MATCH
        - NULL != value → NON-MATCH
        - value != NULL → NON-MATCH
        - value == value → MATCH
        - value != different_value → NON-MATCH

    Notes:
        NULL handling above applies to compared values. NULL join keys do not
        match. Unmatched outer-join rows are differences even when compared values
        are NULL. Duplicate keys can multiply joined rows; check key uniqueness
        first when expecting one-to-one alignment.

    Example:
        >>> compare_columns_by_key(source, target, ["id"], ["amount"], join_type="full").show()
    """
    ...

def compare_schema(
    df_a: DataFrame,
    df_b: DataFrame,
    name_a: str = "Dataset A",
    name_b: str = "Dataset B",
) -> DataFrame:
    """Compare schemas of two DataFrames and identify differences.

    Args:
        df_a: First DataFrame to compare
        df_b: Second DataFrame to compare
        name_a: Display name for first dataset (default: "Dataset A")
        name_b: Display name for second dataset (default: "Dataset B")

    Returns:
        DataFrame with schema comparison results showing:
        - Columns in both datasets with matching or different data types
        - Columns only in dataset A
        - Columns only in dataset B
    """
    ...

def numeric_diff_check(
    df_a: DataFrame,
    df_b: DataFrame,
    key_cols: List[str],
    numeric_cols: Optional[List[str]] = None,
    tolerance: float = 0.0,
    show_summary_only: bool = False,
    diff_type: Literal["absolute", "percentage", "both"] = "absolute",
) -> DataFrame:
    """Compare numeric values between two datasets by key columns and analyze differences.

    Args:
        df_a: First DataFrame to compare
        df_b: Second DataFrame to compare
        key_cols: List of columns to join on (keys that identify matching records)
        numeric_cols: List of numeric columns to compare. If None, auto-detects numeric columns
        tolerance: Absolute tolerance for considering values as equal (default: 0.0)
        show_summary_only: If True, returns only summary statistics per column.
                          If False, returns detailed differences (default: False)
        diff_type: Type of difference to calculate:
                  - "absolute": Absolute difference abs(a - b)
                  - "percentage": Percentage difference ((a - b) / b * 100)
                  - "both": Both absolute and percentage

    Returns:
        If show_summary_only=True:
            DataFrame with summary per column:
            - column: Column name
            - total_compared: Number of records compared
            - matching_values: Count of matching values (within tolerance)
            - differing_values: Count of differing values
            - diff_percentage: Percentage of differing values
            - avg_absolute_diff: Average absolute difference
            - max_absolute_diff: Maximum absolute difference
            - min_absolute_diff: Minimum absolute difference (excluding zeros)

        If show_summary_only=False:
            DataFrame with detailed differences:
            - [key_cols]: Key columns that identify the record
            - [column]_a: Original value from df_a
            - [column]_b: Original value from df_b
            - [column]_diff: Absolute difference
            - [column]_pct_diff: Percentage difference (if diff_type includes percentage)

    Features:
        - Joins datasets by key columns
        - Compares numeric values with configurable tolerance
        - Calculates absolute and/or percentage differences
        - Provides both summary statistics and detailed differences
        - Identifies which records differ and by how much
        - Summary statistics are aggregated together

    Notes:
        Uses an inner join: unmatched keys are omitted, NULL keys do not join,
        and duplicate keys can multiply compared rows. Values with a NULL operand
        count neither as matching nor differing; use null_rate for missingness.
        Tolerance applies to absolute differences, including percentage mode.
        Percentage difference is signed and NULL when the value in B is zero.
        Details always include absolute differences; diff_type controls the extra
        percentage column. Summary output is the same for every diff_type.

    Example:
        >>> numeric_diff_check(source, target, ["id"], ["amount"], tolerance=0.01).show()
    """
    ...

def find_key_set(
    tables: Dict[str, DataFrame],
    keys_df: DataFrame,
    key_column: str,
    candidate_columns: Optional[List[str]] = None,
) -> DataFrame:
    """Search multiple tables/columns for a provided set of key values.

    Args:
        tables: Mapping of table_name -> DataFrame to inspect
        keys_df: DataFrame containing keys to search for
        key_column: Column in keys_df that contains key values
        candidate_columns: Optional explicit column list to check in each table.
                          If None, all columns in each table are checked.
                          Missing columns are skipped; an empty list checks no columns.

    Returns:
        DataFrame with one row per checked table/column:
        - table_name
        - column_name
        - total_keys
        - matched_count
        - missing_count
        - coverage_pct

        If no candidate columns exist, returns an empty DataFrame with this schema.
        Existing columns without matches return zero coverage. Spark chooses the
        join strategy using its configuration and statistics; no broadcast is forced.

    Notes:
        Both search keys and candidate values are cast to strings and deduplicated;
        NULL values are excluded. Coverage measures distinct search keys, not rows.
        Raises ValueError for an empty tables mapping or no non-null search keys.

    Example:
        >>> find_key_set({"orders": orders}, customers, "id", ["customer_id"]).show()
    """
    ...
