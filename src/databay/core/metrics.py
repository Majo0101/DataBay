from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def compare_datasets(
    df_a: DataFrame,
    df_b: DataFrame,
    cols: List[str],
    name_a: str = "Dataset A",
    name_b: str = "Dataset B"
) -> DataFrame:
    """
    Compare two DataFrames and compute similarity metrics.
    
    Args:
        df_a: First DataFrame to compare
        df_b: Second DataFrame to compare
        cols: List of columns to include in comparison (use ["*"] for all columns)
        name_a: Display name for first dataset (default: "Dataset A")
        name_b: Display name for second dataset (default: "Dataset B")
    
    Returns:
        DataFrame with metrics: row counts, differences, match percentages, Jaccard similarity
    """
    
    # Expand "*" to actual column list from df_a to ensure consistent column order
    if cols == ["*"]:
        cols = df_a.columns

    a = df_a.select(*cols).cache()
    b = df_b.select(*cols).cache()

    cnt_a = a.count()
    cnt_b = b.count()

    diff_a = a.exceptAll(b).count()
    diff_b = b.exceptAll(a).count()

    common_rows = cnt_a - diff_a
    union_rows = cnt_a + cnt_b - common_rows

    a_to_b_pct = (common_rows / cnt_a * 100) if cnt_a else 0.0
    b_to_a_pct = ((cnt_b - diff_b) / cnt_b * 100) if cnt_b else 0.0
    jaccard_pct = (common_rows / union_rows * 100) if union_rows else 0.0
    size_ratio_pct = (cnt_a / cnt_b * 100) if cnt_b else 0.0

    rows = [
        ("row_count", name_a, cnt_a, None),
        ("row_count", name_b, cnt_b, None),
        ("diff_rows", name_a, diff_a, None),
        ("diff_rows", name_b, diff_b, None),
        ("common_rows", "both", common_rows, None),
        ("union_rows", "both", union_rows, None),
        ("match_pct", f"{name_a}→{name_b}", common_rows, round(a_to_b_pct, 4)),
        ("match_pct", f"{name_b}→{name_a}", cnt_b - diff_b, round(b_to_a_pct, 4)),
        ("jaccard_pct", "both", common_rows, round(jaccard_pct, 4)),
        ("size_ratio_pct", f"{name_a}→{name_b}", cnt_a, round(size_ratio_pct, 4)),
    ]

    result = df_a.sparkSession.createDataFrame(
        rows,
        ["metric", "scope", "count_value", "percent_value"]
    )

    a.unpersist()
    b.unpersist()

    return result


def compare_columns_by_key(
    df_a: DataFrame,
    df_b: DataFrame,
    key_cols: List[str],
    compare_cols: List[str],
    show_summary_only: bool = True
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
    
    if compare_cols == ["*"]:
        compare_cols = [c for c in df_a.columns if c not in key_cols]

    a = df_a.alias("a")
    b = df_b.alias("b")

    joined = a.join(b, key_cols, "inner")

    if show_summary_only:
        # Compute total AND match counts in ONE pass
        agg_exprs = [F.count("*").alias("total")]
        for c in compare_cols:
            agg_exprs.append(
                F.sum(
                    F.when(
                        (F.col(f"a.{c}") == F.col(f"b.{c}")) |
                        (F.col(f"a.{c}").isNull() & F.col(f"b.{c}").isNull()),
                        1
                    ).otherwise(0)
                ).alias(c)
            )

        result_dict = joined.select(agg_exprs).collect()[0].asDict()
        total = result_dict.pop("total")

        rows = []
        for c in compare_cols:
            m = result_dict[c] or 0  # Handle None when join is empty
            match_pct = round(m / total * 100, 4) if total > 0 else 0.0
            rows.append((c, total, m, total - m, match_pct))

        return joined.sparkSession.createDataFrame(
            rows,
            ["column", "total_rows", "matching_rows", "non_matching_rows", "match_%"]
        )
    
    else:
        # Return detailed differences
        select_exprs = key_cols.copy()
        
        for c in compare_cols:
            col_a = F.col(f"a.{c}")
            col_b = F.col(f"b.{c}")
            
            # Check if values match (including NULL == NULL)
            # Explicitly handle NULLs to avoid NULL in result
            is_match = F.when(
                col_a.isNull() & col_b.isNull(), True
            ).when(
                col_a.isNull() | col_b.isNull(), False
            ).otherwise(
                col_a == col_b
            )
            
            select_exprs.extend([
                col_a.alias(f"{c}_a"),
                col_b.alias(f"{c}_b"),
                is_match.alias(f"{c}_match")
            ])
        
        detailed = joined.select(select_exprs)
        
        # Filter to only rows where at least one column differs
        diff_filter = F.lit(False)
        for c in compare_cols:
            diff_filter = diff_filter | ~F.col(f"{c}_match")
        
        detailed = detailed.filter(diff_filter)
        
        return detailed
    

def compare_schema(
    df_a: DataFrame,
    df_b: DataFrame,
    name_a: str = "Dataset A",
    name_b: str = "Dataset B"
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
    
    schema_a = {field.name: str(field.dataType) for field in df_a.schema.fields}
    schema_b = {field.name: str(field.dataType) for field in df_b.schema.fields}
    
    all_cols = sorted(set(schema_a.keys()) | set(schema_b.keys()))
    
    rows = []
    for col in all_cols:
        type_a = schema_a.get(col)
        type_b = schema_b.get(col)
        
        if type_a and type_b:
            if type_a == type_b:
                status = "MATCH"
            else:
                status = "TYPE_MISMATCH"
        elif type_a:
            status = f"ONLY_IN_{name_a.upper().replace(' ', '_')}"
        else:
            status = f"ONLY_IN_{name_b.upper().replace(' ', '_')}"
        
        rows.append((col, type_a, type_b, status))
    
    return df_a.sparkSession.createDataFrame(
        rows,
        ["column_name", f"type_in_{name_a.lower().replace(' ', '_')}", 
         f"type_in_{name_b.lower().replace(' ', '_')}", "status"]
    )

def numeric_diff_check(
    df_a: DataFrame,
    df_b: DataFrame,
    key_cols: List[str],
    numeric_cols: Optional[List[str]] = None,
    tolerance: float = 0.0,
    show_summary_only: bool = False,
    diff_type: str = "absolute"
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
    # --- helper: numeric type check ---
    def _is_numeric_type(simple_type: str) -> bool:
        if simple_type.startswith("decimal"):
            return True
        return simple_type in [
            "int",
            "bigint",
            "double",
            "float",
            "smallint",
            "tinyint",
        ]

    # --- validate key columns ---
    for col in key_cols:
        if col not in df_a.columns:
            raise ValueError(f"Column '{col}' not found in df_a")
        if col not in df_b.columns:
            raise ValueError(f"Column '{col}' not found in df_b")

    # --- build type maps ---
    a_types = {f.name: f.dataType.simpleString() for f in df_a.schema.fields}
    b_types = {f.name: f.dataType.simpleString() for f in df_b.schema.fields}

    # --- auto-detect numeric columns ---
    if numeric_cols is None:
        numeric_cols = [
            name
            for name, dtype in a_types.items()
            if _is_numeric_type(dtype)
            and name in b_types
            and _is_numeric_type(b_types[name])
            and name not in key_cols
        ]
    else:
        # Keep only numeric columns present in both dataframes
        numeric_cols = [
            name
            for name in numeric_cols
            if name in a_types
            and name in b_types
            and _is_numeric_type(a_types[name])
            and _is_numeric_type(b_types[name])
            and name not in key_cols
        ]

    if not numeric_cols:
        raise ValueError("No numeric columns found to compare.")

    cols_to_select = key_cols + numeric_cols

    a = df_a.select(cols_to_select).alias("a")
    b = df_b.select(cols_to_select).alias("b")

    joined = a.join(
        broadcast(b),
        on=key_cols,
        how="inner"
    )

    if show_summary_only:

        agg_exprs = [F.count("*").alias("total_compared")]

        for col in numeric_cols:
            col_a = F.col(f"a.{col}")
            col_b = F.col(f"b.{col}")

            raw_diff = F.abs(col_a - col_b)

            # --- KEY FIX: everything below tolerance is treated as 0 ---
            abs_diff = F.when(raw_diff <= tolerance, F.lit(0.0)).otherwise(raw_diff)

            is_diff = raw_diff > tolerance

            agg_exprs.extend([
                F.sum(F.when(~is_diff, 1).otherwise(0)).alias(f"{col}_matching"),
                F.sum(F.when(is_diff, 1).otherwise(0)).alias(f"{col}_differing"),
                F.avg(abs_diff).alias(f"{col}_avg_diff"),
                F.max(abs_diff).alias(f"{col}_max_diff"),
                F.min(F.when(abs_diff > 0, abs_diff)).alias(f"{col}_min_diff"),
            ])

        stats = joined.agg(*agg_exprs).collect()[0]
        total = stats["total_compared"]

        rows = []
        for col in numeric_cols:
            matching = stats[f"{col}_matching"]
            differing = stats[f"{col}_differing"]

            rows.append((
                col,
                total,
                matching,
                differing,
                round((differing / total * 100) if total > 0 else 0.0, 2),
                float(f"{stats[f'{col}_avg_diff'] or 0.0:.6f}"),
                float(f"{stats[f'{col}_max_diff'] or 0.0:.6f}"),
                float(f"{stats[f'{col}_min_diff'] or 0.0:.6f}"),
            ))

        return df_a.sparkSession.createDataFrame(
            rows,
            [
                "column",
                "total_compared",
                "matching_values",
                "differing_values",
                "diff_percentage",
                "avg_absolute_diff",
                "max_absolute_diff",
                "min_absolute_diff",
            ],
        )

    else:

        select_exprs = key_cols.copy()

        for col in numeric_cols:
            col_a = F.col(f"a.{col}")
            col_b = F.col(f"b.{col}")

            raw_diff = F.abs(col_a - col_b)
            abs_diff = F.when(raw_diff <= tolerance, F.lit(0.0)).otherwise(raw_diff)
            is_diff = raw_diff > tolerance

            select_exprs.extend([
                col_a.alias(f"{col}_a"),
                col_b.alias(f"{col}_b"),
                abs_diff.alias(f"{col}_diff"),
                is_diff.alias(f"{col}_is_diff"),
            ])

            if diff_type in ["percentage", "both"]:
                pct_diff = F.when(
                    col_b != 0,
                    ((col_a - col_b) / col_b * 100)
                ).otherwise(None)

                select_exprs.append(pct_diff.alias(f"{col}_pct_diff"))

        detailed = joined.select(select_exprs)

        diff_filter = F.lit(False)
        for col in numeric_cols:
            diff_filter = diff_filter | F.col(f"{col}_is_diff")

        detailed = detailed.filter(diff_filter)

        output_cols = key_cols.copy()
        for col in numeric_cols:
            output_cols.extend([f"{col}_a", f"{col}_b", f"{col}_diff"])
            if diff_type in ["percentage", "both"]:
                output_cols.append(f"{col}_pct_diff")

        return detailed.select(output_cols)


def find_key_set(
    tables: Dict[str, DataFrame],
    keys_df: DataFrame,
    key_column: str,
    candidate_columns: Optional[List[str]] = None,
) -> DataFrame:
    """
    Search multiple tables/columns for a provided set of key values.

    Args:
        tables: Mapping of table_name -> DataFrame to inspect
        keys_df: DataFrame containing keys to search for
        key_column: Column in keys_df that contains key values
        candidate_columns: Optional explicit column list to check in each table.
                          If None, all columns in each table are checked.

    Returns:
        DataFrame with one row per checked table/column:
        - table_name
        - column_name
        - total_keys
        - matched_count
        - missing_count
        - coverage_pct
    """
    if not tables:
        raise ValueError("tables must be a non-empty dict of table_name -> DataFrame")
    if not isinstance(key_column, str) or not key_column:
        raise ValueError("key_column must be a non-empty string")
    if key_column not in keys_df.columns:
        raise ValueError(f"Column '{key_column}' not found in keys_df")

    first_df = next(iter(tables.values()))
    spark = keys_df.sparkSession if keys_df.sparkSession is not None else first_df.sparkSession

    search_keys_df = (
        keys_df
        .select(F.col(key_column).cast("string").alias("key_value"))
        .where(F.col("key_value").isNotNull())
        .distinct()
    )
    total_keys = search_keys_df.count()
    if total_keys == 0:
        raise ValueError("keys_df must contain at least one non-null key value")

    result_rows = []

    for table_name, df in tables.items():
        cols_to_check = candidate_columns if candidate_columns is not None else df.columns

        for col_name in cols_to_check:
            if col_name not in df.columns:
                continue

            column_values = (
                df.select(F.col(col_name).cast("string").alias("key_value"))
                .where(F.col("key_value").isNotNull())
                .distinct()
            )

            matched_count = search_keys_df.join(
                broadcast(column_values),
                on="key_value",
                how="inner",
            ).count()
            missing_count = total_keys - matched_count

            coverage_pct = round((matched_count / total_keys * 100) if total_keys > 0 else 0.0, 4)

            result_rows.append(
                (
                    table_name,
                    col_name,
                    total_keys,
                    matched_count,
                    missing_count,
                    coverage_pct,
                )
            )

    return spark.createDataFrame(
        result_rows,
        [
            "table_name",
            "column_name",
            "total_keys",
            "matched_count",
            "missing_count",
            "coverage_pct",
        ],
    )
