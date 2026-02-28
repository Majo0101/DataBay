from __future__ import annotations

from typing import Dict, List, Optional

from pyspark.sql import DataFrame


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
