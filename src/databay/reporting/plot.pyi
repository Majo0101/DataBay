from __future__ import annotations

from typing import Dict, List, Literal, Mapping, Optional, Sequence, SupportsFloat, Tuple, TypedDict, Union

import pandas as pd
import plotly.graph_objs as go


class OverlapInput(TypedDict):
    left_count: SupportsFloat
    right_count: SupportsFloat
    overlap_count: SupportsFloat


def dataframe_to_dict(
    df: Union[pd.DataFrame, "DataFrame"],
    key_col: str,
    value_col: str,
    duplicate_policy: Literal["error", "first", "last"] = "error",
) -> Dict[str, float]:
    ...


def plot_match_percentage(
    data: Mapping[str, SupportsFloat],
    title: str = "Column Match Percentage: PROD vs TEST",
    height: int = 600,
    show_figure: bool = False,
    x_range: Optional[Tuple[int, int]] = None,
    margin_left: int = 200,
    value_mode: Literal["auto", "percent", "ratio"] = "auto",
    sort: bool = False,
    ascending: bool = True,
    top_n: Optional[int] = None,
    color_scale: Optional[List[Tuple[float, str]]] = None,
    colorbar_ticks: Optional[Dict[str, Sequence[Union[int, str]]]] = None,
    metric_label: str = "Match %",
    show_reference_lines: bool = False,
    reference_lines: Optional[Sequence[float]] = None,
) -> go.Figure:
    ...


def plot_match_percentage_sorted(
    data: Mapping[str, SupportsFloat],
    title: str = "Column Match Percentage: PROD vs TEST (Sorted)",
    ascending: bool = True,
    **kwargs,
) -> go.Figure:
    ...


def plot_overlap(
    data: Union[OverlapInput, Mapping[str, object]],
    title: str = "Overlap Comparison",
    left_count_key: str = "left_count",
    right_count_key: str = "right_count",
    overlap_count_key: str = "overlap_count",
    left_name: str = "Left",
    right_name: str = "Right",
    show_figure: bool = False,
    height: int = 500,
    overlap_color: str = "#43a047",
    left_only_color: str = "#f9a825",
    right_only_color: str = "#e53935",
) -> go.Figure:
    ...
