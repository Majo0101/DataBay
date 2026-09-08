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
) -> DataFrame: ...


def duplicate_check(
    df: DataFrame,
    cols: Optional[List[str]] = None,
    top_n: int = 10,
    show_summary_only: bool = False,
    check_nulls: bool = False,
) -> DataFrame: ...


def pk_uniqueness_check(df: DataFrame, pk_cols: List[str]) -> DataFrame: ...


def regex_check(
    df: DataFrame,
    rules: Dict[str, str],
    show_summary_only: bool = True,
    top_n: int = 10,
) -> DataFrame: ...


def row_level_rules(
    df: DataFrame,
    rules: Dict[str, str],
    show_summary_only: bool = True,
    top_n: int = 10,
) -> DataFrame: ...


def cardinality_check(
    df: DataFrame,
    col_left: Union[str, List[str]],
    col_right: Union[str, List[str]],
    show_summary_only: bool = True,
    summary_view: str = "full",
    top_n: int = 10,
) -> DataFrame: ...


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
) -> DataFrame: ...
