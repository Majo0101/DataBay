from typing import Dict, List, Literal, Optional, Tuple, Union, overload

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from ._columns import literal_col, quote_identifier


@overload
def select_informative_columns(
    df: DataFrame,
    min_non_null_percentage: float = 0.0,
    min_distinct_values: int = 1,
    preserve: Optional[List[str]] = None,
    treat_blank_as_null: bool = True,
    return_report: Literal[False] = False,
) -> DataFrame: ...


@overload
def select_informative_columns(
    df: DataFrame,
    min_non_null_percentage: float = 0.0,
    min_distinct_values: int = 1,
    preserve: Optional[List[str]] = None,
    treat_blank_as_null: bool = True,
    return_report: Literal[True] = True,
) -> Tuple[DataFrame, DataFrame]: ...


def select_informative_columns(
    df: DataFrame,
    min_non_null_percentage: float = 0.0,
    min_distinct_values: int = 1,
    preserve: Optional[List[str]] = None,
    treat_blank_as_null: bool = True,
    return_report: bool = False,
) -> Union[DataFrame, Tuple[DataFrame, DataFrame]]:
    """Return a projection containing columns with useful observed values.

    Empty columns are always removed unless explicitly preserved. Optional
    thresholds can also remove sparse or constant columns. String blanks are
    treated as missing values by default. All column statistics are calculated
    in one Spark aggregation.

    Args:
        df: Spark DataFrame to profile and project.
        min_non_null_percentage: Minimum populated percentage from 0 to 100.
        min_distinct_values: Minimum number of distinct populated values.
        preserve: Columns to retain regardless of their statistics.
        treat_blank_as_null: Treat empty and whitespace-only strings as missing.
        return_report: Return ``(clean_df, report_df)`` when True.

    Returns:
        A projected DataFrame, or that DataFrame plus a decision report with
        column name, type, population statistics, distinct count, and status.
    """
    if not df.columns:
        raise ValueError("df must contain at least one column")
    if not isinstance(min_non_null_percentage, (int, float)) or isinstance(
        min_non_null_percentage, bool
    ):
        raise ValueError("min_non_null_percentage must be a number from 0 to 100")
    if not 0 <= float(min_non_null_percentage) <= 100:
        raise ValueError("min_non_null_percentage must be between 0 and 100")
    if not isinstance(min_distinct_values, int) or isinstance(min_distinct_values, bool):
        raise ValueError("min_distinct_values must be an integer >= 1")
    if min_distinct_values < 1:
        raise ValueError("min_distinct_values must be >= 1")

    preserve = [] if preserve is None else preserve
    if not isinstance(preserve, list) or any(
        not isinstance(name, str) or not name for name in preserve
    ):
        raise ValueError("preserve must be a list of non-empty column names")
    missing_preserved = [name for name in preserve if name not in df.columns]
    if missing_preserved:
        raise ValueError(f"Preserved column '{missing_preserved[0]}' not found in df")

    preserve_set = set(preserve)
    schema_by_name = {field.name: field.dataType for field in df.schema.fields}
    aggregate_expressions = [F.count("*").alias("__db_total_rows__")]

    for index, column_name in enumerate(df.columns):
        column = literal_col(column_name)
        populated = column.isNotNull()
        if treat_blank_as_null and isinstance(schema_by_name[column_name], StringType):
            populated = populated & (F.length(F.trim(column)) > 0)

        aggregate_expressions.extend(
            [
                F.sum(F.when(populated, 1).otherwise(0)).alias(f"__db_non_null_{index}"),
                F.countDistinct(F.when(populated, column)).alias(f"__db_distinct_{index}"),
            ]
        )

    statistics = df.agg(*aggregate_expressions).collect()[0]
    total_rows = int(statistics["__db_total_rows__"] or 0)
    kept_columns = []
    report_rows = []

    for index, column_name in enumerate(df.columns):
        non_null_count = int(statistics[f"__db_non_null_{index}"] or 0)
        distinct_count = int(statistics[f"__db_distinct_{index}"] or 0)
        non_null_percentage = round(
            (non_null_count / total_rows * 100) if total_rows else 0.0,
            4,
        )

        if column_name in preserve_set:
            status = "PRESERVED"
        elif non_null_count == 0:
            status = "DROPPED_EMPTY"
        elif non_null_percentage < float(min_non_null_percentage):
            status = "DROPPED_SPARSE"
        elif distinct_count < min_distinct_values:
            status = "DROPPED_CONSTANT"
        else:
            status = "KEPT"

        if status in {"PRESERVED", "KEPT"}:
            kept_columns.append(column_name)

        report_rows.append(
            (
                column_name,
                schema_by_name[column_name].simpleString(),
                total_rows,
                non_null_count,
                non_null_percentage,
                distinct_count,
                status,
            )
        )

    if not kept_columns:
        raise ValueError(
            "No informative columns remain; lower the thresholds or preserve at least one column"
        )

    clean_df = df.select(*[literal_col(c) for c in kept_columns])
    if not return_report:
        return clean_df

    report_df = df.sparkSession.createDataFrame(
        report_rows,
        [
            "column_name",
            "data_type",
            "total_rows",
            "non_null_count",
            "non_null_percentage",
            "distinct_values",
            "status",
        ],
    )
    return clean_df, report_df

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
            F.sum(F.when(literal_col(col_name).isNull(), 1).otherwise(0)).alias(col_name)
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
    
    result_schema = StructType([
        StructField("column_name", StringType(), True),
        StructField("total_records", LongType(), True),
        StructField("null_records", LongType(), True),
        StructField("non_null_records", LongType(), True),
        StructField("null_percentage", DoubleType(), True),
        StructField("data_type", StringType(), True),
    ])
    result = df.sparkSession.createDataFrame(rows, result_schema)
    
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
    
    if top_n < 0:
        raise ValueError("top_n must be >= 0")

    # Ensure cols is a list (handle None case)
    cols_to_check: List[str]
    if cols is None:
        cols_to_check = df.columns
    else:
        cols_to_check = cols

    if not cols_to_check or any((not isinstance(c, str) or not c) for c in cols_to_check):
        raise ValueError("cols must be a non-empty list of non-empty strings")

    for c in cols_to_check:
        if c not in df.columns:
            raise ValueError(f"Column '{c}' not found in DataFrame")
    
    total_rows = df.count()
    
    # Check for NULLs if requested
    null_stats = {}
    if check_nulls:
        null_exprs = [
            F.sum(F.when(literal_col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in cols_to_check
        ]
        null_result = df.select(null_exprs).collect()[0].asDict()
        null_stats = {k: v or 0 for k, v in null_result.items()}
    
    # Group by selected columns and count occurrences
    grouped = df.groupBy(*[literal_col(c) for c in cols_to_check]).agg(F.count("*").alias("duplicate_count"))
    
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
        return result.select(*[literal_col(c) for c in ordered_cols])


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
        show_summary_only: If True, returns one-row-per-column summary.
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
    if top_n < 0:
        raise ValueError("top_n must be >= 0")

    for col_name, pattern in rules.items():
        if col_name not in df.columns:
            raise ValueError(f"Column '{col_name}' not found in DataFrame")
        if not isinstance(pattern, str) or not pattern:
            raise ValueError(f"Pattern for column '{col_name}' must be a non-empty string")

    total_rows = df.count()

    # NULL is considered non-matching for regex validation
    match_exprs = {
        col_name: F.when(
            literal_col(col_name).isNotNull() & literal_col(col_name).cast("string").rlike(pattern),
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
            .select(literal_col(col_name).cast("string").alias("non_matching_value"))
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
        show_summary_only: If True, returns one-row-per-rule summary.
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
    if top_n < 0:
        raise ValueError("top_n must be >= 0")

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
        row_repr_expr = F.to_json(F.struct(*[literal_col(c) for c in df.columns]))
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


def cardinality_check(
    df: DataFrame,
    col_left: Union[str, List[str]],
    col_right: Union[str, List[str]],
    show_summary_only: bool = True,
    summary_view: str = "full",
    top_n: int = 10,
) -> DataFrame:
    """
    Profile cardinality relationship between two columns.

    Args:
        df: DataFrame to analyze
        col_left: Left column name(s)
        col_right: Right column name(s)
        show_summary_only: If True, returns one-row summary with relationship class.
                          If False, returns top violating values (default: True)
        summary_view: Summary output mode when show_summary_only=True:
                      - "full": all metrics
                      - "short": total rows, non-null pairs, cardinality only
        top_n: Number of top violating values to return in detailed mode (default: 10)

    Returns:
        If show_summary_only=True:
            One-row DataFrame with:
            - col_left
            - col_right
            - total_rows
            - non_null_pair_rows
            - distinct_left
            - distinct_right
            - distinct_pairs
            - left_to_right_max
            - right_to_left_max
            - relationship_type (1:1, 1:N, N:1, N:N, EMPTY)

        If show_summary_only=False:
            DataFrame with violating pair keys:
            - [left key columns]
            - [right key columns] (right side columns may get "_right" suffix on name clash)
            - rows_in_left
            - rows_in_right
    """
    def _normalize_cols(cols: Union[str, List[str]], label: str) -> List[str]:
        if isinstance(cols, str):
            if not cols:
                raise ValueError(f"{label} must be a non-empty string or non-empty list of strings")
            return [cols]
        if isinstance(cols, list) and cols and all(isinstance(c, str) and c for c in cols):
            return cols
        raise ValueError(f"{label} must be a non-empty string or non-empty list of strings")

    left_cols = _normalize_cols(col_left, "col_left")
    right_cols = _normalize_cols(col_right, "col_right")

    for c in left_cols:
        if c not in df.columns:
            raise ValueError(f"Column '{c}' not found in DataFrame")
    for c in right_cols:
        if c not in df.columns:
            raise ValueError(f"Column '{c}' not found in DataFrame")
    if summary_view not in {"full", "short"}:
        raise ValueError("summary_view must be 'full' or 'short'")
    if top_n < 0:
        raise ValueError("top_n must be >= 0")

    total_rows = df.count()
    left_not_null = F.lit(True)
    for c in left_cols:
        left_not_null = left_not_null & literal_col(c).isNotNull()
    right_not_null = F.lit(True)
    for c in right_cols:
        right_not_null = right_not_null & literal_col(c).isNotNull()

    pairs = df.select(
        F.struct(*[literal_col(c) for c in left_cols]).alias("_left_key"),
        F.struct(*[literal_col(c) for c in right_cols]).alias("_right_key"),
    ).filter(
        left_not_null & right_not_null
    )

    non_null_pair_rows = pairs.count()
    distinct_left = pairs.select("_left_key").distinct().count()
    distinct_right = pairs.select("_right_key").distinct().count()
    distinct_pairs = pairs.select("_left_key", "_right_key").distinct().count()

    left_counts = pairs.groupBy("_left_key").agg(F.count_distinct("_right_key").alias("distinct_partner_count"))
    right_counts = pairs.groupBy("_right_key").agg(F.count_distinct("_left_key").alias("distinct_partner_count"))

    left_to_right_max = (
        left_counts.agg(F.max("distinct_partner_count").alias("m")).collect()[0]["m"] or 0
    )
    right_to_left_max = (
        right_counts.agg(F.max("distinct_partner_count").alias("m")).collect()[0]["m"] or 0
    )

    if non_null_pair_rows == 0:
        relationship_type = "EMPTY"
    elif left_to_right_max <= 1 and right_to_left_max <= 1:
        relationship_type = "1:1"
    elif left_to_right_max > 1 and right_to_left_max <= 1:
        relationship_type = "1:N"
    elif left_to_right_max <= 1 and right_to_left_max > 1:
        relationship_type = "N:1"
    else:
        relationship_type = "N:N"

    if show_summary_only:
        if summary_view == "short":
            return df.sparkSession.createDataFrame(
                [(
                    total_rows,
                    non_null_pair_rows,
                    distinct_pairs,
                    relationship_type,
                )],
                [
                    "total_rows",
                    "non_null_pair_rows",
                    "distinct_pairs",
                    "relationship_type",
                ],
            )

        return df.sparkSession.createDataFrame(
            [(
                ",".join(left_cols),
                ",".join(right_cols),
                total_rows,
                non_null_pair_rows,
                distinct_left,
                distinct_right,
                distinct_pairs,
                left_to_right_max,
                right_to_left_max,
                relationship_type,
            )],
            [
                "col_left",
                "col_right",
                "total_rows",
                "non_null_pair_rows",
                "distinct_left",
                "distinct_right",
                "distinct_pairs",
                "left_to_right_max",
                "right_to_left_max",
                "relationship_type",
            ],
        )

    if top_n == 0:
        left_schema = ", ".join([f"{quote_identifier(c)} string" for c in left_cols])
        right_out_names = []
        for c in right_cols:
            right_out_names.append(f"{c}_right" if c in left_cols else c)
        right_schema = ", ".join([f"{quote_identifier(c)} string" for c in right_out_names])
        return df.sparkSession.createDataFrame(
            [],
            f"{left_schema}, {right_schema}, rows_in_left long, rows_in_right long",
        )

    pairs_distinct = pairs.select("_left_key", "_right_key").distinct()
    detailed = (
        pairs_distinct
        .join(left_counts.withColumnRenamed("distinct_partner_count", "rows_in_left"), on="_left_key", how="inner")
        .join(right_counts.withColumnRenamed("distinct_partner_count", "rows_in_right"), on="_right_key", how="inner")
        .filter((F.col("rows_in_left") > 1) | (F.col("rows_in_right") > 1))
    )

    for c in left_cols:
        detailed = detailed.withColumn(c, F.col("_left_key")[c].cast("string"))

    right_out_names = []
    for c in right_cols:
        out_name = f"{c}_right" if c in left_cols else c
        right_out_names.append(out_name)
        detailed = detailed.withColumn(out_name, F.col("_right_key")[c].cast("string"))

    detailed = (
        detailed
        .select(*[literal_col(c) for c in left_cols + right_out_names], "rows_in_left", "rows_in_right")
        .orderBy(F.greatest(F.col("rows_in_left"), F.col("rows_in_right")).desc(), *[literal_col(c) for c in left_cols])
        .limit(top_n)
    )
    return detailed


def cardinality_check_tables(
    df_a: DataFrame,
    df_b: DataFrame,
    cols_a: Union[str, List[str]],
    cols_b: Union[str, List[str]],
    show_summary_only: bool = True,
    summary_view: str = "full",
    name_a: str = "a",
    name_b: str = "b",
    top_n: int = 10,
) -> DataFrame:
    """
    Profile cardinality relationship of key set(s) across two tables.

    Args:
        df_a: Left DataFrame
        df_b: Right DataFrame
        cols_a: Column name(s) in df_a
        cols_b: Column name(s) in df_b
        show_summary_only: If True, returns one-row summary.
                          If False, returns top violating keys (default: True)
        summary_view: Summary output mode when show_summary_only=True:
                      - "full": all metrics
                      - "short": total rows, overlap, cardinality only
        name_a: Label for dataset A used in output column names
        name_b: Label for dataset B used in output column names
        top_n: Number of top violating keys in detailed mode (default: 10)

    Returns:
        If show_summary_only=True:
            One-row DataFrame with cardinality and overlap metrics.
        If show_summary_only=False:
            DataFrame with columns:
            - [cols_a] (key columns from df_a naming)
            - rows_in_<name_a>
            - rows_in_<name_b>
    """
    def _normalize_cols(cols: Union[str, List[str]], label: str) -> List[str]:
        if isinstance(cols, str):
            if not cols:
                raise ValueError(f"{label} must be a non-empty string or non-empty list of strings")
            return [cols]
        if isinstance(cols, list) and cols and all(isinstance(c, str) and c for c in cols):
            return cols
        raise ValueError(f"{label} must be a non-empty string or non-empty list of strings")

    left_cols = _normalize_cols(cols_a, "cols_a")
    right_cols = _normalize_cols(cols_b, "cols_b")
    if len(left_cols) != len(right_cols):
        raise ValueError("cols_a and cols_b must contain the same number of columns")

    for c in left_cols:
        if c not in df_a.columns:
            raise ValueError(f"Column '{c}' not found in df_a")
    for c in right_cols:
        if c not in df_b.columns:
            raise ValueError(f"Column '{c}' not found in df_b")
    if summary_view not in {"full", "short"}:
        raise ValueError("summary_view must be 'full' or 'short'")
    if not isinstance(name_a, str) or not name_a:
        raise ValueError("name_a must be a non-empty string")
    if not isinstance(name_b, str) or not name_b:
        raise ValueError("name_b must be a non-empty string")
    if top_n < 0:
        raise ValueError("top_n must be >= 0")

    def _safe_label(value: str) -> str:
        return "".join(ch if ch.isalnum() else "_" for ch in value.strip().lower())

    label_a = _safe_label(name_a)
    label_b = _safe_label(name_b)

    left_not_null = F.lit(True)
    for c in left_cols:
        left_not_null = left_not_null & literal_col(c).isNotNull()
    right_not_null = F.lit(True)
    for c in right_cols:
        right_not_null = right_not_null & literal_col(c).isNotNull()

    keys_a = (
        df_a.select(F.struct(*[literal_col(c) for c in left_cols]).alias("_key"))
        .filter(left_not_null)
    )
    keys_b = (
        df_b.select(F.struct(*[literal_col(c) for c in right_cols]).alias("_key"))
        .filter(right_not_null)
    )

    total_rows_a = df_a.count()
    total_rows_b = df_b.count()
    non_null_rows_a = keys_a.count()
    non_null_rows_b = keys_b.count()

    rows_in_a_col = f"rows_in_{label_a}"
    rows_in_b_col = f"rows_in_{label_b}"
    total_rows_a_col = f"total_rows_{label_a}"
    total_rows_b_col = f"total_rows_{label_b}"

    grouped_a = keys_a.groupBy("_key").agg(F.count("*").alias(rows_in_a_col))
    grouped_b = keys_b.groupBy("_key").agg(F.count("*").alias(rows_in_b_col))

    distinct_keys_a = grouped_a.count()
    distinct_keys_b = grouped_b.count()

    overlap = grouped_a.join(grouped_b, on="_key", how="inner")
    overlap_distinct_keys = overlap.count()

    max_rows_per_key_a = (overlap.agg(F.max(rows_in_a_col).alias("m")).collect()[0]["m"] or 0)
    max_rows_per_key_b = (overlap.agg(F.max(rows_in_b_col).alias("m")).collect()[0]["m"] or 0)

    if overlap_distinct_keys == 0:
        relationship_type = "EMPTY"
    elif max_rows_per_key_a <= 1 and max_rows_per_key_b <= 1:
        relationship_type = "1:1"
    elif max_rows_per_key_a > 1 and max_rows_per_key_b <= 1:
        relationship_type = "N:1"
    elif max_rows_per_key_a <= 1 and max_rows_per_key_b > 1:
        relationship_type = "1:N"
    else:
        relationship_type = "N:N"

    coverage_a_to_b_pct = round((overlap_distinct_keys / distinct_keys_a * 100) if distinct_keys_a > 0 else 0.0, 4)
    coverage_b_to_a_pct = round((overlap_distinct_keys / distinct_keys_b * 100) if distinct_keys_b > 0 else 0.0, 4)

    if show_summary_only:
        if summary_view == "short":
            return df_a.sparkSession.createDataFrame(
                [(
                    total_rows_a,
                    total_rows_b,
                    overlap_distinct_keys,
                    relationship_type,
                )],
                [
                    total_rows_a_col,
                    total_rows_b_col,
                    "overlap_distinct_keys",
                    "relationship_type",
                ],
            )

        return df_a.sparkSession.createDataFrame(
            [(
                ",".join(left_cols),
                ",".join(right_cols),
                total_rows_a,
                total_rows_b,
                non_null_rows_a,
                non_null_rows_b,
                distinct_keys_a,
                distinct_keys_b,
                overlap_distinct_keys,
                coverage_a_to_b_pct,
                coverage_b_to_a_pct,
                max_rows_per_key_a,
                max_rows_per_key_b,
                relationship_type,
            )],
            [
                "cols_a",
                "cols_b",
                total_rows_a_col,
                total_rows_b_col,
                f"non_null_rows_{label_a}",
                f"non_null_rows_{label_b}",
                f"distinct_keys_{label_a}",
                f"distinct_keys_{label_b}",
                "overlap_distinct_keys",
                f"coverage_{label_a}_to_{label_b}_pct",
                f"coverage_{label_b}_to_{label_a}_pct",
                f"max_rows_per_key_{label_a}",
                f"max_rows_per_key_{label_b}",
                "relationship_type",
            ],
        )

    if top_n == 0:
        key_schema = ", ".join([f"{quote_identifier(c)} string" for c in left_cols])
        return df_a.sparkSession.createDataFrame(
            [],
            f"{key_schema}, {rows_in_a_col} long, {rows_in_b_col} long",
        )

    detailed = (
        overlap
        .filter((literal_col(rows_in_a_col) > 1) | (literal_col(rows_in_b_col) > 1))
    )

    for c in left_cols:
        detailed = detailed.withColumn(c, F.col("_key")[c].cast("string"))

    detailed = (
        detailed
        .select(*[literal_col(c) for c in left_cols], rows_in_a_col, rows_in_b_col)
        .orderBy(F.greatest(literal_col(rows_in_a_col), literal_col(rows_in_b_col)).desc(), *[literal_col(c) for c in left_cols])
        .limit(top_n)
    )
    return detailed
