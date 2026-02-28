from __future__ import annotations
from typing import Dict, List, Optional
from pyspark.sql import DataFrame

def compare_datasets(
    df_a: DataFrame,
    df_b: DataFrame,
    cols: List[str],
    name_a: str = "Dataset A",
    name_b: str = "Dataset B",
) -> DataFrame:
    """
    Compare two DataFrames and compute similarity metrics.
    
    Args:
        df_a: First DataFrame to compare
        df_b: Second DataFrame to compare
        cols: List of columns to include in comparison
        name_a: Display name for first dataset (default: "Dataset A")
        name_b: Display name for second dataset (default: "Dataset B")
    
    Returns:
        DataFrame with metrics: row counts, differences, match percentages, Jaccard similarity
    """
    ...

def compare_columns_by_key(
    df_a: DataFrame,
    df_b: DataFrame,
    key_cols: List[str],
    compare_cols: List[str],
    show_summary_only: bool = True,
) -> DataFrame:
    """
    Compare specific columns between two DataFrames joined by key columns.
    
    Args:
        df_a: First DataFrame to compare
        df_b: Second DataFrame to compare
        key_cols: List of columns to join on
        compare_cols: List of columns to compare, or ["*"] for all non-key columns
        show_summary_only: If True, returns summary statistics per column.
                          If False, returns detailed differences (default: True)
    
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
    """
    ...

def compare_schema(
    df_a: DataFrame,
    df_b: DataFrame,
    name_a: str = "Dataset A",
    name_b: str = "Dataset B",
) -> DataFrame:
    """
    Compare schemas of two DataFrames and identify differences.
    
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
    diff_type: str = "absolute",
) -> DataFrame:
    """
    Compare numeric values between two datasets by key columns and analyze differences.
    
    Args:
        df_a: First DataFrame to compare
        df_b: Second DataFrame to compare
        key_cols: List of columns to join on (keys that identify matching records)
        numeric_cols: List of numeric columns to compare. If None, auto-detects numeric columns
        tolerance: Absolute tolerance for considering values as equal (default: 0.0)
        show_summary_only: If True, returns only summary statistics per column. 
                          If False, returns detailed differences (default: False)
        diff_type: Type of difference to calculate:
                  - "absolute": Absolute difference (a - b)
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
        - Handles null values gracefully
        - Optimized for performance with single-pass aggregations
    """
    ...

def find_key_set(
    tables: Dict[str, DataFrame],
    keys_df: DataFrame,
    key_column: str,
    candidate_columns: Optional[List[str]] = None,
) -> DataFrame:
    """
    Search multiple tables/columns for a provided set of key values.
    """
    ...
