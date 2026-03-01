import math
from typing import Dict, List, Literal, Mapping, Optional, Sequence, SupportsFloat, Tuple, TypedDict, Union
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go


class OverlapInput(TypedDict):
    left_count: SupportsFloat
    right_count: SupportsFloat
    overlap_count: SupportsFloat


def _coerce_finite_float(value: object, field_name: str) -> float:
    """Convert a numeric-like value to float and reject NaN/inf."""
    try:
        out = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"'{field_name}' must be numeric") from None
    if not math.isfinite(out):
        raise ValueError(f"'{field_name}' must be a finite number")
    return out


def _to_pandas(df: Union[pd.DataFrame, 'DataFrame']) -> pd.DataFrame:
    """
    Convert DataFrame to pandas if needed.
    
    Handles both pandas DataFrames and PySpark DataFrames.
    
    Parameters
    ----------
    df : Union[pd.DataFrame, pyspark.sql.DataFrame]
        Input DataFrame (Pandas or PySpark)
    
    Returns
    -------
    pd.DataFrame
        Pandas DataFrame
    
    Raises
    ------
    TypeError
        If input is not a supported DataFrame type
    """
    # Already a pandas DataFrame
    if isinstance(df, pd.DataFrame):
        return df
    
    # Check if it's a PySpark DataFrame by checking for toPandas method
    if hasattr(df, 'toPandas'):
        return df.toPandas()
    
    # Unsupported type
    raise TypeError(
        f"Expected pandas.DataFrame or PySpark DataFrame, got {type(df).__name__}. "
        f"If using PySpark, ensure the DataFrame has a 'toPandas()' method."
    )


# Data Quality Color Scale - Based on Testing Best Practices
# Red (0-60%): Critical/Failed - Major data quality issues
# Orange (60-92%): Warning - Needs attention
# Yellow (92-99%): Caution - Minor discrepancies
# Green (99-100%): Pass - Good to perfect quality
DATA_QUALITY_COLORS = [
    (0.00, "#e53935"),   # vivid red - critical
    (0.60, "#ff6f00"),   # bright orange-red - warning
    (0.80, "#ffa726"),   # orange - attention needed
    (0.92, "#ffeb3b"),   # bright yellow - caution
    (0.97, "#f0f4c3"),   # very light yellow - minor issues
    (0.99, "#dce775"),   # lime - good
    (0.999, "#aed581"),  # light green - very good
    (1.00, "#66bb6a")    # medium green - perfect match
]

# Quality thresholds for colorbar labels
QUALITY_THRESHOLDS = {
    'tickvals': [70, 95, 100],
    'ticktext': ["Critical", "Warning", "Perfect"]
}

def _prepare_plot_df(
    df: pd.DataFrame,
    column_col: str,
    match_col: str,
    value_mode: Literal["auto", "percent", "ratio"],
    sort: bool,
    ascending: bool,
    top_n: Optional[int],
) -> pd.DataFrame:
    """Normalize metric values and optionally sort/filter rows for plotting."""
    if top_n is not None and top_n < 1:
        raise ValueError("top_n must be >= 1 when provided")

    plot_df = df.copy()
    plot_df[match_col] = pd.to_numeric(plot_df[match_col], errors="coerce")
    if plot_df[match_col].isna().any():
        raise ValueError(f"Column '{match_col}' contains non-numeric values")

    if value_mode == "ratio":
        plot_df[match_col] = plot_df[match_col] * 100
    elif value_mode == "auto" and plot_df[match_col].max() <= 1.0:
        plot_df[match_col] = plot_df[match_col] * 100

    if sort:
        plot_df = plot_df.sort_values(match_col, ascending=ascending)

    if top_n is not None:
        plot_df = plot_df.head(top_n)

    # Keep explicit category order in chart
    plot_df[column_col] = plot_df[column_col].astype(str)
    return plot_df


def dataframe_to_dict(
    df: Union[pd.DataFrame, "DataFrame"],
    key_col: str,
    value_col: str,
    duplicate_policy: Literal["error", "first", "last"] = "error",
) -> Dict[str, float]:
    """
    Convert a two-column DataFrame into a dictionary mapping key -> numeric value.

    Works with both Pandas and PySpark DataFrames.

    duplicate_policy:
    - "error": raise when duplicate keys are found
    - "first": keep first occurrence of each key
    - "last": keep last occurrence of each key
    """
    pdf = _to_pandas(df)

    if key_col not in pdf.columns:
        raise ValueError(f"Column '{key_col}' not found in DataFrame")
    if value_col not in pdf.columns:
        raise ValueError(f"Column '{value_col}' not found in DataFrame")
    if duplicate_policy not in {"error", "first", "last"}:
        raise ValueError("duplicate_policy must be one of: 'error', 'first', 'last'")

    work = pdf[[key_col, value_col]].copy()
    work[key_col] = work[key_col].astype(str).str.strip()
    if (work[key_col] == "").any():
        raise ValueError(f"Column '{key_col}' contains empty key values")

    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    if work[value_col].isna().any():
        raise ValueError(f"Column '{value_col}' contains non-numeric values")
    if not work[value_col].map(math.isfinite).all():
        raise ValueError(f"Column '{value_col}' must contain finite numbers")

    if duplicate_policy == "error":
        dupes = work[work[key_col].duplicated(keep=False)][key_col].unique().tolist()
        if dupes:
            sample = ", ".join(map(str, dupes[:5]))
            raise ValueError(f"Duplicate keys found in '{key_col}': {sample}")
    elif duplicate_policy == "first":
        work = work.drop_duplicates(subset=[key_col], keep="first")
    else:
        work = work.drop_duplicates(subset=[key_col], keep="last")

    return dict(zip(work[key_col].tolist(), work[value_col].astype(float).tolist()))


def plot_match_percentage(
    data: Mapping[str, SupportsFloat],
    title: str = 'Column Match Percentage: PROD vs TEST',
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
    """
    Create a horizontal bar chart showing match percentages with data quality color coding.
    
    This function visualizes data quality metrics using a color-coded horizontal bar chart.
    Colors range from red (critical quality issues) to green (perfect match), following
    data testing best practices for quality thresholds.
    
    Expects a dictionary mapping column name -> match value.
    
    Parameters
    ----------
    data : Mapping[str, SupportsFloat]
        Mapping of column name to match value.
    title : str, optional
        Chart title, by default 'Column Match Percentage: PROD vs TEST'
    height : int, optional
        Chart height in pixels, by default 600
    show_figure : bool, optional
        If True, displays the figure immediately, by default False
    x_range : Optional[Tuple[int, int]], optional
        Custom x-axis range as (min, max), by default (0, 115)
    margin_left : int, optional
        Left margin in pixels for y-axis labels, by default 200
    value_mode : Literal["auto", "percent", "ratio"], optional
        Metric scale interpretation:
        - "auto": treat values <=1 as ratios and convert to percent
        - "percent": use values as-is (expected 0..100)
        - "ratio": always convert by multiplying by 100
    sort : bool, optional
        If True, sort bars by match values before plotting, by default False
    ascending : bool, optional
        Sort direction when sort=True, by default True
    top_n : Optional[int], optional
        If provided, keep only first N rows after sorting/filtering
    color_scale : Optional[List[Tuple[float, str]]], optional
        Custom Plotly continuous color scale. Defaults to DATA_QUALITY_COLORS.
    colorbar_ticks : Optional[Dict[str, Sequence[Union[int, str]]]], optional
        Custom colorbar ticks, e.g. {"tickvals":[70,95,100], "ticktext":[...]}.
        Defaults to QUALITY_THRESHOLDS.
    metric_label : str, optional
        Axis/color metric label, by default "Match %"
    show_reference_lines : bool, optional
        If True, draw vertical reference lines, by default False
    reference_lines : Optional[Sequence[float]], optional
        X positions for reference lines. Defaults to [95.0, 99.0] if enabled.
    
    Returns
    -------
    plotly.graph_objs.Figure
        The generated Plotly figure object that can be further customized or displayed
    
    Raises
    ------
    ValueError
        If input dictionary is empty, has empty keys, or contains non-finite values.
    
    Examples
    --------
    >>> match_data = {"col1": 100.0, "col2": 95.5, "col3": 89.2}
    >>> fig = plot_match_percentage(match_data)
    >>> fig.show()
    
    >>> # With custom parameters
    >>> fig = plot_match_percentage(
    ...     match_data,
    ...     title='Data Quality Check',
    ...     height=800,
    ...     show_figure=True
    ... )
    """
    if not data:
        raise ValueError("Input 'data' must not be empty")

    columns = [str(k) for k in data.keys()]
    if any(not c.strip() for c in columns):
        raise ValueError("All keys in 'data' must be non-empty strings")

    values = [_coerce_finite_float(v, f"data['{columns[i]}']") for i, v in enumerate(data.values())]
    plot_df = pd.DataFrame({"column": columns, "match_%": values})
    
    plot_df = _prepare_plot_df(
        df=plot_df,
        column_col="column",
        match_col="match_%",
        value_mode=value_mode,
        sort=sort,
        ascending=ascending,
        top_n=top_n,
    )

    # Set default x-axis range if not provided
    if x_range is None:
        x_range = (0, 115)

    if color_scale is None:
        color_scale = DATA_QUALITY_COLORS
    if colorbar_ticks is None:
        colorbar_ticks = QUALITY_THRESHOLDS

    # Create the bar chart
    fig = px.bar(
        plot_df,
        y="column",
        x="match_%",
        orientation='h',
        text="match_%",
        color="match_%",
        color_continuous_scale=color_scale,
        labels={"match_%": metric_label, "column": 'Column Name'},
        title=f'<b>{title}</b>',
        height=height,
        range_color=[0, 100]
    )
    
    # Update trace styling
    fig.update_traces(
        texttemplate='%{text:.2f}%',
        textposition='outside',
        textfont_size=11,
        hovertemplate='<b>%{y}</b><br>Match: %{x:.2f}%<extra></extra>'
    )
    
    # Apply default config to remove unnecessary toolbar buttons
    fig.update_layout(
        modebar=dict(
            remove=['zoom2d', 'pan2d', 'select2d', 'lasso2d', 'autoScale2d', 'resetScale2d',
                   'toggleSpikelines', 'hoverCompareCartesian', 'hoverClosestCartesian']
        )
    )
    
    # Update layout for professional appearance
    fig.update_layout(
        xaxis=dict(
            range=x_range,
            showgrid=True,
            gridcolor='lightgray',
            title='Match Percentage (%)',
            title_font=dict(size=13),
            domain=[0, 0.85]
        ),
        yaxis=dict(
            showgrid=False,
            categoryorder='array',
            categoryarray=plot_df["column"].tolist(),
            title='Column Name',
            title_font=dict(size=13)
        ),
        font=dict(size=12, family='Arial, sans-serif'),
        plot_bgcolor='white',
        paper_bgcolor='white',
        margin=dict(l=margin_left, r=150, t=60, b=50),
        coloraxis_colorbar=dict(
            title="",
            title_font=dict(size=12),
            thickness=15,
            len=1.0,
            x=0.88,
            xanchor='left',
            **colorbar_ticks
        ),
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Arial, sans-serif"
        )
    )

    if show_reference_lines:
        lines = list(reference_lines) if reference_lines is not None else [95.0, 99.0]
        for x_val in lines:
            fig.add_vline(
                x=x_val,
                line_width=1,
                line_dash="dash",
                line_color="#9e9e9e",
            )
    
    # Display figure if requested
    if show_figure:
        fig.show(config={'displaylogo': False})
    
    return fig


def plot_match_percentage_sorted(
    data: Mapping[str, SupportsFloat],
    title: str = 'Column Match Percentage: PROD vs TEST (Sorted)',
    ascending: bool = True,
    **kwargs
) -> go.Figure:
    """
    Create a sorted horizontal bar chart showing match percentages.
    
    This is a convenience function that sorts the dictionary input before plotting.
    
    Parameters
    ----------
    data : Mapping[str, SupportsFloat]
        Mapping of column name to match value.
    title : str, optional
        Chart title, by default includes '(Sorted)'
    ascending : bool, optional
        Sort in ascending order (lowest match % first), by default True
    **kwargs
        Additional keyword arguments passed to plot_match_percentage()
    
    Returns
    -------
    plotly.graph_objs.Figure
        The generated Plotly figure object
    
    Examples
    --------
    >>> fig = plot_match_percentage_sorted({"col1": 99.0, "col2": 87.5}, ascending=False)
    """
    kwargs.setdefault("sort", True)
    kwargs.setdefault("ascending", ascending)
    return plot_match_percentage(data, title=title, **kwargs)


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
    """
    Two-bar funnel-style overlap chart for dataset A vs dataset B.

    Expects a dictionary with three numeric values:
    - `left_count` (or custom `left_count_key`)
    - `right_count` (or custom `right_count_key`)
    - `overlap_count` (or custom `overlap_count_key`)

    The chart contains two horizontal stacked bars:
    - Bar for dataset A (`left_name`): overlap (green) + only A (yellow)
    - Bar for dataset B (`right_name`): overlap (green) + only B (red)

    Parameters
    ----------
    data : Mapping[str, object]
        Dictionary with overlap summary counts.
    title : str
        Chart title.
    left_count_key : str
        Key name for dataset A total count in `data`.
    right_count_key : str
        Key name for dataset B total count in `data`.
    overlap_count_key : str
        Key name for overlap/matched count in `data`.
    left_name : str
        Display name for dataset A (left side).
    right_name : str
        Display name for dataset B (right side).
    show_figure : bool
        If True, calls ``fig.show()`` before returning.
    height : int
        Chart height in pixels.
    overlap_color : str
        Hex color for overlap/matched segment (default green).
    left_only_color : str
        Hex color for rows exclusive to dataset A (default yellow).
    right_only_color : str
        Hex color for rows exclusive to dataset B (default red).

    Returns
    -------
    plotly.graph_objs.Figure

    Raises
    ------
    ValueError
        If required keys are missing, contain non-numeric values,
        negative values, or if overlap > either side's total.
    """
    required = [left_count_key, right_count_key, overlap_count_key]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Missing required key(s): {', '.join(missing)}")

    try:
        left_total = _coerce_finite_float(data[left_count_key], left_count_key)
        right_total = _coerce_finite_float(data[right_count_key], right_count_key)
        overlap_total = _coerce_finite_float(data[overlap_count_key], overlap_count_key)
    except KeyError:
        # Defensive fallback, should not happen due to explicit missing check above.
        raise ValueError("Missing required key(s) in input data") from None

    if left_total < 0:
        raise ValueError(f"'{left_count_key}' must be >= 0")
    if right_total < 0:
        raise ValueError(f"'{right_count_key}' must be >= 0")
    if overlap_total < 0:
        raise ValueError(f"'{overlap_count_key}' must be >= 0")

    if overlap_total > left_total or overlap_total > right_total:
        raise ValueError(
            f"'{overlap_count_key}' cannot be greater than '{left_count_key}' or '{right_count_key}'"
        )

    left_only_total = left_total - overlap_total
    right_only_total = right_total - overlap_total

    y_labels = [left_name, right_name]
    max_val = max(left_total, right_total, 1.0)
    left_overlap_pct = overlap_total / left_total * 100 if left_total else 0.0
    right_overlap_pct = overlap_total / right_total * 100 if right_total else 0.0

    fig = go.Figure()

    fig.add_bar(
        name="Overlap",
        y=y_labels,
        x=[overlap_total, overlap_total],
        orientation="h",
        marker_color=overlap_color,
        marker_line_width=0,
        hovertemplate=[
            (
                f"<b>{left_name}</b><br>"
                f"Total: {left_total:,.0f}<br>"
                f"Overlap: {overlap_total:,.0f} ({left_overlap_pct:.1f}%)<extra></extra>"
            ),
            (
                f"<b>{right_name}</b><br>"
                f"Total: {right_total:,.0f}<br>"
                f"Overlap: {overlap_total:,.0f} ({right_overlap_pct:.1f}%)<extra></extra>"
            ),
        ],
    )

    fig.add_bar(
        name=f"Only in {left_name}",
        y=y_labels,
        x=[left_only_total, 0],
        orientation="h",
        marker_color=left_only_color,
        marker_line_width=0,
        hovertemplate=[
            (
                f"<b>{left_name}</b><br>"
                f"Only in {left_name}: {left_only_total:,.0f}<extra></extra>"
            ),
            "<extra></extra>",
        ],
    )

    fig.add_bar(
        name=f"Only in {right_name}",
        y=y_labels,
        x=[0, right_only_total],
        orientation="h",
        marker_color=right_only_color,
        marker_line_width=0,
        hovertemplate=[
            "<extra></extra>",
            (
                f"<b>{right_name}</b><br>"
                f"Only in {right_name}: {right_only_total:,.0f}<extra></extra>"
            ),
        ],
    )

    fig.update_layout(
        title=dict(text=f"<b>{title}</b>", font=dict(size=15)),
        barmode="stack",
        height=max(height, 280),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(size=12, family="Arial, sans-serif"),
        margin=dict(l=40, r=40, t=70, b=50),
        bargap=0.45,
        xaxis=dict(
            showgrid=True,
            gridcolor="#eeeeee",
            zeroline=True,
            zerolinecolor="#616161",
            zerolinewidth=1,
            range=[0, max_val * 1.15],
            tickformat=",",
            title="Row count",
        ),
        yaxis=dict(
            showgrid=False,
            categoryorder="array",
            categoryarray=[right_name, left_name],
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5,
        ),
        modebar=dict(
            remove=[
                "zoom2d", "pan2d", "select2d", "lasso2d",
                "autoScale2d", "resetScale2d",
                "toggleSpikelines", "hoverCompareCartesian", "hoverClosestCartesian",
            ]
        ),
    )

    fig.update_traces(texttemplate="%{x:,.0f}", textposition="inside")

    fig.add_annotation(
        x=left_total,
        y=left_name,
        text=f"Total: {left_total:,.0f}",
        showarrow=False,
        xanchor="left",
        xshift=6,
        font=dict(size=10, color="#424242"),
    )
    fig.add_annotation(
        x=right_total,
        y=right_name,
        text=f"Total: {right_total:,.0f}",
        showarrow=False,
        xanchor="left",
        xshift=6,
        font=dict(size=10, color="#424242"),
    )

    if show_figure:
        fig.show(config={"displaylogo": False})

    return fig
