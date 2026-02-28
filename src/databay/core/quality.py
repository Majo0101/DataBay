from typing import Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def null_rate(
    df: DataFrame,
    threshold: Optional[float] = None,
    sort_desc: bool = True
) -> DataFrame:
    """
    Calculate null/missing value statistics for each column in a DataFrame.
    
    Args:
        df: DataFrame to analyze for null values
        threshold: Optional minimum null percentage to include in results (e.g., 5.0 for 5%)
                  If None, all columns are returned
        sort_desc: If True, sort by null percentage descending (worst first). Default: True
    
    Returns:
        DataFrame with columns:
        - column_name: Name of the column
        - total_records: Total number of rows
        - null_records: Count of null/missing values
        - non_null_records: Count of non-null values
        - null_percentage: Percentage of null values (rounded to 4 decimals)
        - data_type: Column data type
    
    Features:
        - Single-pass aggregation for efficiency
        - Shows data types to identify potential type-related null issues
        - Optional threshold filtering to focus on problematic columns
        - Sorted by null percentage to highlight worst columns first
    """
    
    total_count = df.count()
    
    # Build aggregation expressions for all columns in one pass
    null_counts = []
    for col_name in df.columns:
        null_counts.append(
            F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(col_name)
        )
    
    # Execute aggregation once
    result_dict = df.select(null_counts).collect()[0].asDict()
    
    # Get schema info
    schema_map = {field.name: str(field.dataType) for field in df.schema.fields}
    
    # Build result rows
    rows = []
    for col_name in df.columns:
        null_count = result_dict[col_name] or 0  # Handle None when DataFrame is empty
        non_null_count = total_count - null_count
        null_pct = round((null_count / total_count * 100) if total_count > 0 else 0.0, 4)
        
        # Apply threshold filter if specified
        if threshold is not None and null_pct < threshold:
            continue
        
        rows.append((
            col_name,
            total_count,
            null_count,
            non_null_count,
            null_pct,
            schema_map.get(col_name, "UNKNOWN")
        ))
    
    result = df.sparkSession.createDataFrame(
        rows,
        ["column_name", "total_records", "null_records", "non_null_records", "null_percentage", "data_type"]
    )
    
    # Sort by null percentage if requested
    if sort_desc:
        result = result.orderBy(F.col("null_percentage").desc())
    
    return result

def duplicate_check(
    df: DataFrame,
    cols: Optional[List[str]] = None,
    top_n: int = 10,
    show_summary_only: bool = False,
    check_nulls: bool = False
) -> DataFrame:
    """
    Analyze duplicate records in a DataFrame based on specified columns.
    
    Args:
        df: DataFrame to analyze for duplicates
        cols: List of columns to check for duplicates. If None, checks all columns
        top_n: Number of top duplicate combinations to show (default: 10)
        show_summary_only: If True, returns only summary statistics. If False, returns 
                          detailed duplicate combinations (default: False)
        check_nulls: If True, includes NULL statistics in summary output (default: False)
    
    Returns:
        If show_summary_only=True:
            DataFrame with overall statistics:
            - metric: Description of the metric
            - value: Numeric value
            - percentage: Percentage value where applicable
            - Includes NULL counts per column if check_nulls=True
        
        If show_summary_only=False:
            DataFrame with top duplicate combinations:
            - duplicate_count: Number of times this combination appears
            - [column values]: The actual values for each column in the combination
            - sorted by duplicate_count descending
    
    Features:
        - Identifies duplicate rows based on column combination
        - Shows top N most duplicated value combinations
        - Provides summary statistics (total, unique, duplicate counts and percentages)
        - Optional NULL value checking (useful for primary key validation)
        - Efficient groupBy aggregation
        - Works with any column combination
    
    Notes:
        - NULL values ARE grouped together (NULL == NULL in PySpark groupBy)
        - When check_nulls=True, reports NULL counts to help identify data quality issues
    """
    
    # Ensure cols is a list (handle None case)
    cols_to_check: List[str]
    if cols is None:
        cols_to_check = df.columns
    else:
        cols_to_check = cols
    
    total_rows = df.count()
    
    # Check for NULLs if requested
    null_stats = {}
    if check_nulls:
        null_exprs = [
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in cols_to_check
        ]
        null_result = df.select(null_exprs).collect()[0].asDict()
        null_stats = {k: v or 0 for k, v in null_result.items()}
    
    # Group by selected columns and count occurrences
    grouped = df.groupBy(*cols_to_check).agg(F.count("*").alias("duplicate_count"))
    
    # Get statistics
    unique_combinations = grouped.count()
    duplicates_df = grouped.filter(F.col("duplicate_count") > 1)
    duplicate_combinations = duplicates_df.count()
    
    # Calculate duplicate rows (rows that are part of duplicate combinations)
    duplicate_rows = duplicates_df.select(
        F.sum(F.col("duplicate_count")).alias("total")
    ).collect()[0]["total"] or 0
    
    unique_rows = total_rows - duplicate_rows
    
    if show_summary_only:
        # Return summary statistics
        dup_pct = round((duplicate_rows / total_rows * 100) if total_rows > 0 else 0.0, 4)
        unique_pct = round((unique_rows / total_rows * 100) if total_rows > 0 else 0.0, 4)
        dup_combo_pct = round((duplicate_combinations / unique_combinations * 100) if unique_combinations > 0 else 0.0, 4)
        
        summary_rows = [
            ("total_rows", total_rows, None),
            ("rows_appearing_once", unique_rows, unique_pct),
            ("rows_in_duplicate_groups", duplicate_rows, dup_pct),
            ("distinct_value_combinations", unique_combinations, None),
            ("combinations_with_duplicates", duplicate_combinations, dup_combo_pct),
            ("columns_checked", len(cols_to_check), None),
        ]
        
        # Add NULL statistics if requested
        if check_nulls:
            total_nulls = sum(null_stats.values())
            null_pct = round((total_nulls / (total_rows * len(cols_to_check)) * 100) if total_rows > 0 else 0.0, 4)
            summary_rows.append(("total_null_values", total_nulls, null_pct))
            
            # Add per-column NULL counts
            for col in cols_to_check:
                col_null_count = null_stats.get(col, 0)
                col_null_pct = round((col_null_count / total_rows * 100) if total_rows > 0 else 0.0, 4)
                summary_rows.append((f"null_in_{col}", col_null_count, col_null_pct))
        
        return df.sparkSession.createDataFrame(
            summary_rows,
            ["metric", "value", "percentage"]
        )
    
    else:
        # Return top N duplicate combinations
        result = duplicates_df.orderBy(F.col("duplicate_count").desc()).limit(top_n)
        
        # Reorder columns: duplicate_count first, then the checked columns
        ordered_cols = ["duplicate_count"] + cols_to_check
        return result.select(*ordered_cols)


def pk_uniqueness_check(df: DataFrame, pk_cols: List[str]) -> DataFrame:
    """
    Check if primary key columns contain only unique values (no duplicates) and no NULLs.
    
    Args:
        df: DataFrame to validate
        pk_cols: List of primary key column names
    
    Returns:
        DataFrame with summary statistics showing:
        - total_rows: Total number of records
        - rows_appearing_once: Records with unique PK values
        - rows_in_duplicate_groups: Records with duplicate PK values
        - distinct_value_combinations: Number of unique PK combinations
        - combinations_with_duplicates: Number of PK values that appear multiple times
        - total_null_values: Total NULL count across all PK columns
        - null_in_[column]: NULL count per PK column
    
    Notes:
        - This is a wrapper around duplicate_check() optimized for PK validation
        - A valid primary key should have:
          * rows_in_duplicate_groups = 0 (no duplicates)
          * total_null_values = 0 (no NULLs)
        - Automatically enables NULL checking (check_nulls=True)
    
    Example:
        >>> result = pk_uniqueness_check(df, ["customer_id"])
        >>> result.show()
    """
    return duplicate_check(
        df,
        cols=pk_cols,
        show_summary_only=True,
        check_nulls=True
    )


def regex_check(
    df: DataFrame,
    rules: Dict[str, str],
    show_summary_only: bool = True,
    top_n: int = 10,
) -> DataFrame:
    """
    Validate one or more columns against regex patterns.

    Args:
        df: DataFrame to validate
        rules: Mapping of column_name -> regex pattern
        show_summary_only: If True, returns one-row summary.
                          If False, returns non-matching values (default: True)
        top_n: Number of top non-matching values to show per column when
               show_summary_only=False (default: 10)

    Returns:
        If show_summary_only=True:
            DataFrame with one row per checked column:
            - column_name
            - pattern
            - total_rows
            - matching_rows
            - non_matching_rows
            - match_percentage

        If show_summary_only=False:
            DataFrame with non-matching values:
            - column_name
            - pattern
            - non_matching_value
            - non_matching_count
    """
    if not rules:
        raise ValueError("rules must be a non-empty dict of column_name -> regex pattern")

    for col_name, pattern in rules.items():
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in DataFrame")
        if not isinstance(pattern, str) or not pattern:
            raise ValueError(f"Pattern for column '{col_name}' must be a non-empty string")

    total_rows = df.count()

    # NULL is considered non-matching for regex validation
    match_exprs = {
        col_name: F.when(
            F.col(col_name).isNotNull() & F.col(col_name).cast("string").rlike(pattern),
            1
        ).otherwise(0)
        for col_name, pattern in rules.items()
    }

    if show_summary_only:
        agg_exprs = [F.sum(expr).alias(col_name) for col_name, expr in match_exprs.items()]
        agg_result = df.select(agg_exprs).collect()[0].asDict()

        summary_rows = [
            (
                col_name,
                pattern,
                total_rows,
                agg_result.get(col_name) or 0,
                total_rows - (agg_result.get(col_name) or 0),
                round(((agg_result.get(col_name) or 0) / total_rows * 100) if total_rows > 0 else 0.0, 4),
            )
            for col_name, pattern in rules.items()
        ]

        return df.sparkSession.createDataFrame(
            summary_rows,
            [
                "column_name",
                "pattern",
                "total_rows",
                "matching_rows",
                "non_matching_rows",
                "match_percentage",
            ],
        )

    # Detailed mode: return non-matching values and their frequency
    detailed = None
    for rule_order, (col_name, pattern) in enumerate(rules.items()):
        invalid = (
            df.filter(match_exprs[col_name] == 0)
            .select(F.col(col_name).cast("string").alias("non_matching_value"))
            .na.fill({"non_matching_value": "<NULL>"})
            .groupBy("non_matching_value")
            .agg(F.count("*").alias("non_matching_count"))
            .withColumn("column_name", F.lit(col_name))
            .withColumn("pattern", F.lit(pattern))
            .withColumn("_rule_order", F.lit(rule_order))
            .select("column_name", "pattern", "non_matching_value", "non_matching_count", "_rule_order")
            .orderBy(F.col("non_matching_count").desc())
            .limit(top_n)
        )
        detailed = invalid if detailed is None else detailed.unionByName(invalid)

    if detailed is None:
        return df.sparkSession.createDataFrame(
            [],
            "column_name string, pattern string, non_matching_value string, non_matching_count long",
        )

    return (
        detailed
        .orderBy(F.col("_rule_order"), F.col("non_matching_count").desc())
        .drop("_rule_order")
    )


def row_level_rules(
    df: DataFrame,
    rules: Dict[str, str],
    show_summary_only: bool = True,
    top_n: int = 10,
) -> DataFrame:
    """
    Validate rows against one or more Spark SQL boolean expressions.

    Args:
        df: DataFrame to validate
        rules: Mapping of rule_name -> Spark SQL boolean expression
        show_summary_only: If True, returns one-row summary per rule.
                          If False, returns failing row representations (default: True)
        top_n: Number of top failing row representations to show per rule when
               show_summary_only=False (default: 10)

    Returns:
        If show_summary_only=True:
            DataFrame with one row per checked rule:
            - column_name (rule_name)
            - pattern (rule_expression)
            - total_rows
            - matching_rows
            - non_matching_rows
            - match_percentage

        If show_summary_only=False:
            DataFrame with failing row representations:
            - column_name (rule_name)
            - pattern (rule_expression)
            - non_matching_value (JSON representation of row)
            - non_matching_count
    """
    if not rules:
        raise ValueError("rules must be a non-empty dict of rule_name -> expression")

    for rule_name, rule_expr in rules.items():
        if not rule_name or not isinstance(rule_name, str):
            raise ValueError("Each rule name must be a non-empty string")
        if not rule_expr or not isinstance(rule_expr, str):
            raise ValueError("Each rule expression must be a non-empty string")

    total_rows = df.count()

    # Rule is considered matching only when expression evaluates to True.
    # False/NULL are treated as non-matching, same semantics as regex_check.
    match_exprs = {
        rule_name: F.when(F.expr(rule_expr), 1).otherwise(0)
        for rule_name, rule_expr in rules.items()
    }

    if show_summary_only:
        agg_exprs = [F.sum(expr).alias(rule_name) for rule_name, expr in match_exprs.items()]
        agg_result = df.select(agg_exprs).collect()[0].asDict()

        summary_rows = [
            (
                rule_name,
                rule_expr,
                total_rows,
                agg_result.get(rule_name) or 0,
                total_rows - (agg_result.get(rule_name) or 0),
                round(((agg_result.get(rule_name) or 0) / total_rows * 100) if total_rows > 0 else 0.0, 4),
            )
            for rule_name, rule_expr in rules.items()
        ]

        return df.sparkSession.createDataFrame(
            summary_rows,
            [
                "column_name",
                "pattern",
                "total_rows",
                "matching_rows",
                "non_matching_rows",
                "match_percentage",
            ],
        )

    # Detailed mode: return failing rows and their frequency
    detailed = None
    if df.columns:
        row_repr_expr = F.to_json(F.struct(*[F.col(c) for c in df.columns]))
    else:
        row_repr_expr = F.lit("{}")

    for rule_order, (rule_name, rule_expr) in enumerate(rules.items()):
        condition = F.expr(rule_expr)
        invalid = (
            df.filter(~condition | condition.isNull())
            .select(row_repr_expr.alias("non_matching_value"))
            .na.fill({"non_matching_value": "<NULL>"})
            .groupBy("non_matching_value")
            .agg(F.count("*").alias("non_matching_count"))
            .withColumn("column_name", F.lit(rule_name))
            .withColumn("pattern", F.lit(rule_expr))
            .withColumn("_rule_order", F.lit(rule_order))
            .select("column_name", "pattern", "non_matching_value", "non_matching_count", "_rule_order")
            .orderBy(F.col("non_matching_count").desc())
            .limit(top_n)
        )
        detailed = invalid if detailed is None else detailed.unionByName(invalid)

    if detailed is None:
        return df.sparkSession.createDataFrame(
            [],
            "column_name string, pattern string, non_matching_value string, non_matching_count long",
        )

    return (
        detailed
        .orderBy(F.col("_rule_order"), F.col("non_matching_count").desc())
        .drop("_rule_order")
    )
