from __future__ import annotations

from typing import Dict, List, Literal, Optional, Tuple, Union, overload

from pyspark.sql import DataFrame


@overload
def select_informative_columns(
    df: DataFrame,
    min_non_null_percentage: float = 0.0,
    min_distinct_values: int = 1,
    preserve: Optional[List[str]] = None,
    treat_blank_as_null: bool = True,
    return_report: Literal[False] = False,
) -> DataFrame:
    """Return columns with useful observed values.

    Args:
        df: Spark DataFrame to profile and project.
        min_non_null_percentage: Minimum populated percentage from 0 to 100.
        min_distinct_values: Minimum number of distinct populated values.
            Default 1 keeps constant columns. Use 2 to keep columns whose
            populated values vary across rows, useful for reverse engineering.
            With 1 and no report, only populated counts are computed;
            countDistinct is skipped without changing the selected columns.
            Higher thresholds use exact distinct counts, not approximations.
        preserve: Column names to keep regardless of thresholds, even if empty.
        treat_blank_as_null: Treat empty and whitespace-only strings as missing.
        return_report: False returns only the projected DataFrame.

    Empty columns are removed unless preserved. Raises ValueError if no
    columns remain. Original values are retained in the returned DataFrame.
    """
    ...


@overload
def select_informative_columns(
    df: DataFrame,
    min_non_null_percentage: float = 0.0,
    min_distinct_values: int = 1,
    preserve: Optional[List[str]] = None,
    treat_blank_as_null: bool = True,
    return_report: Literal[True] = True,
) -> Tuple[DataFrame, DataFrame]:
    """Return the selected columns and an exact profiling report.

    Args:
        df: Spark DataFrame to profile and project.
        min_non_null_percentage: Minimum populated percentage from 0 to 100.
        min_distinct_values: Minimum number of distinct populated values.
            Default 1 keeps constant columns. Use 2 to keep columns whose
            populated values vary across rows, useful for reverse engineering.
        preserve: Column names to keep regardless of thresholds, even if empty.
        treat_blank_as_null: Treat empty and whitespace-only strings as missing.
        return_report: Pass True to return (clean_df, report_df). The report
            includes column name, type, row count, populated count/percentage,
            exact distinct count and selection status. Distinct counts are
            always computed for the report, including when the threshold is 1.
            Only threshold 1 without a report skips countDistinct.

    Empty columns are removed unless preserved. Raises ValueError if no
    columns remain. Original values are retained in the returned DataFrame.
    No approximate distinct counts are used.
    """
    ...


def null_rate(
    df: DataFrame,
    threshold: Optional[float] = None,
    sort_desc: bool = True,
) -> DataFrame:
    """Calculate null/missing value statistics for each column in a DataFrame.

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
        - Counts rows separately, then aggregates NULL counts across all columns
        - Shows data types to identify potential type-related null issues
        - Optional threshold filtering to focus on problematic columns
        - Sorted by null percentage to highlight worst columns first

    Notes:
        Only SQL NULL counts as missing; blank strings and NaN are not NULL.
        The threshold is inclusive and applied to the rounded percentage.
        If no column reaches it, the report is empty with the same schema.
        Empty input has zero counts and zero percentages.

    Example:
        >>> null_rate(df, threshold=5.0).show()
    """
    ...


def duplicate_check(
    df: DataFrame,
    cols: Optional[List[str]] = None,
    top_n: int = 10,
    show_summary_only: bool = False,
    check_nulls: bool = False,
) -> DataFrame:
    """Analyze duplicate records in a DataFrame based on specified columns.

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
        - rows_in_duplicate_groups includes every row in repeated groups, not
          just extra copies. top_n limits detail output only; 0 returns no details.

    Example:
        >>> duplicate_check(df, cols=["customer_id"], show_summary_only=True).show()
    """
    ...


def pk_uniqueness_check(df: DataFrame, pk_cols: List[str]) -> DataFrame:
    """Check if primary key columns contain only unique values (no duplicates) and no NULLs.

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
        - This is a wrapper around duplicate_check() configured for PK validation
        - A valid primary key should have:
          * rows_in_duplicate_groups = 0 (no duplicates)
          * total_null_values = 0 (no NULLs)
        - Automatically enables NULL checking (check_nulls=True)

    Example:
        >>> result = pk_uniqueness_check(df, ["customer_id"])
        >>> result.show()
    """
    ...


def regex_check(
    df: DataFrame,
    rules: Dict[str, str],
    show_summary_only: bool = True,
    top_n: int = 10,
) -> DataFrame:
    """Validate one or more columns against regex patterns.

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

    Notes:
        Values are cast to strings; NULL fails validation. Patterns use Spark
        rlike semantics: use ^ and $ for a whole-value check. Detail output groups
        invalid values by frequency and represents NULL as "<NULL>".

    Example:
        >>> regex_check(df, {"postcode": "^[0-9]{5}$"}).show()
    """
    ...


def row_level_rules(
    df: DataFrame,
    rules: Dict[str, str],
    show_summary_only: bool = True,
    top_n: int = 10,
) -> DataFrame:
    """Validate rows against one or more Spark SQL boolean expressions.

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

    Notes:
        Only TRUE passes; FALSE and NULL fail. Expressions are Spark SQL, not
        Python. Quote literal column names containing dots with backticks inside
        expressions. top_n limits grouped failing representations per rule.

    Example:
        >>> row_level_rules(df, {"valid_id": "`customer.id` IS NOT NULL"}).show()
    """
    ...


def cardinality_check(
    df: DataFrame,
    col_left: Union[str, List[str]],
    col_right: Union[str, List[str]],
    show_summary_only: bool = True,
    summary_view: str = "full",
    top_n: int = 10,
) -> DataFrame:
    """Profile cardinality relationship between two columns or composite keys.

    Args:
        df: DataFrame to analyze
        col_left: Left column name(s)
        col_right: Right column name(s)
        show_summary_only: If True, returns one-row summary with relationship class.
                          If False, returns top violating values (default: True)
        summary_view: Summary output mode when show_summary_only=True:
                      - "full": all metrics
                      - "short": total rows, non-null pairs, distinct pairs, cardinality
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

    Notes:
        Rows with NULL in any key component are excluded from relationship
        metrics. Repeated copies of the same pair do not increase the number of
        distinct partners. In details, rows_in_left/right count distinct partners,
        not source rows; key values are returned as strings.

    Example:
        >>> cardinality_check(df, "customer_id", "order_id").show()
    """
    ...


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
    """Profile cardinality relationship of key set(s) across two tables.

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

    Notes:
        Composite keys are paired by list position and must have equal lengths.
        NULL-containing keys are excluded. Relationship classification uses only
        keys present in both tables and their row multiplicities. EMPTY means no
        shared non-null keys. Coverage percentages use distinct non-null keys.
        Detail keys are strings. Labels are lowercased and non-alphanumeric
        characters replaced by underscores; choose distinct normalized labels.

    Example:
        >>> cardinality_check_tables(orders, customers, "customer_id", "id").show()
    """
    ...
