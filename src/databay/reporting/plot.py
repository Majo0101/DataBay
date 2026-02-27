from typing import Dict, List, Literal, Optional, Sequence, Tuple, Union
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go


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


def plot_match_percentage(
    df: Union[pd.DataFrame, 'DataFrame'],
    title: str = 'Column Match Percentage: PROD vs TEST',
    height: int = 600,
    column_col: str = 'column',
    match_col: str = 'match_%',
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
    
    **Accepts both Pandas and PySpark DataFrames** - automatically converts PySpark to Pandas.
    
    Parameters
    ----------
    df : Union[pd.DataFrame, pyspark.sql.DataFrame]
        DataFrame containing columns for comparison results.
        Accepts both Pandas DataFrames and PySpark DataFrames.
        Must have columns specified by `column_col` and `match_col`.
    title : str, optional
        Chart title, by default 'Column Match Percentage: PROD vs TEST'
    height : int, optional
        Chart height in pixels, by default 600
    column_col : str, optional
        Name of the column containing column names, by default 'column'
    match_col : str, optional
        Name of the column containing match percentages, by default 'match_%'
    show_figure : bool, optional
        If True, displays the figure immediately, by default False
    x_range : Optional[Tuple[int, int]], optional
        Custom x-axis range as (min, max), by default (0, 105)
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
        If required columns are not present in the DataFrame
    TypeError
        If input is not a supported DataFrame type
    
    Examples
    --------
    >>> import pandas as pd
    >>> 
    >>> # Works with Pandas DataFrames
    >>> df_pandas = pd.DataFrame({
    ...     'column': ['col1', 'col2', 'col3'],
    ...     'match_%': [100.0, 95.5, 89.2]
    ... })
    >>> fig = plot_match_percentage(df_pandas)
    >>> fig.show()
    
    >>> # Works with PySpark DataFrames (auto-converts)
    >>> df_spark = spark.createDataFrame(df_pandas)
    >>> fig = plot_match_percentage(df_spark)  # Auto-converts to Pandas
    >>> fig.show()
    
    >>> # With custom parameters
    >>> fig = plot_match_percentage(
    ...     df_pandas,
    ...     title='Data Quality Check',
    ...     height=800,
    ...     show_figure=True
    ... )
    """
    # Convert to Pandas if needed (handles PySpark DataFrames)
    df = _to_pandas(df)
    
    # Validate required columns
    if column_col not in df.columns:
        raise ValueError(f"Column '{column_col}' not found in DataFrame")
    if match_col not in df.columns:
        raise ValueError(f"Column '{match_col}' not found in DataFrame")
    
    plot_df = _prepare_plot_df(
        df=df,
        column_col=column_col,
        match_col=match_col,
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
        y=column_col,
        x=match_col,
        orientation='h',
        text=match_col,
        color=match_col,
        color_continuous_scale=color_scale,
        labels={match_col: metric_label, column_col: 'Column Name'},
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
            categoryarray=plot_df[column_col].tolist(),
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
    df: Union[pd.DataFrame, 'DataFrame'],
    title: str = 'Column Match Percentage: PROD vs TEST (Sorted)',
    ascending: bool = True,
    **kwargs
) -> go.Figure:
    """
    Create a sorted horizontal bar chart showing match percentages.
    
    This is a convenience function that sorts the data before plotting.
    **Accepts both Pandas and PySpark DataFrames** - automatically converts PySpark to Pandas.
    
    Parameters
    ----------
    df : Union[pd.DataFrame, pyspark.sql.DataFrame]
        DataFrame containing comparison results.
        Accepts both Pandas DataFrames and PySpark DataFrames.
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
    >>> # Works with both Pandas and PySpark DataFrames
    >>> fig = plot_match_percentage_sorted(df, ascending=False)  # Highest first
    >>> fig = plot_match_percentage_sorted(spark_df, ascending=True)  # Lowest first
    """
    kwargs.setdefault("sort", True)
    kwargs.setdefault("ascending", ascending)
    return plot_match_percentage(df, title=title, **kwargs)


def plot_overlap(
    df: Union[pd.DataFrame, "DataFrame"],
    title: str = "Overlap Comparison",
    label_col: str = "label",
    left_count_col: str = "left_count",
    right_count_col: str = "right_count",
    overlap_count_col: str = "overlap_count",
    left_name: str = "Left",
    right_name: str = "Right",
    show_figure: bool = False,
    height: int = 500,
    overlap_color: str = "#43a047",
    left_only_color: str = "#f9a825",
    right_only_color: str = "#e53935",
) -> go.Figure:
    """
    Plot overlap as two stacked horizontal bars (left/right) per label.

    Expects a summary table with counts:
    - label
    - left_count
    - right_count
    - overlap_count
    """
    pdf = _to_pandas(df)

    required = [label_col, left_count_col, right_count_col, overlap_count_col]
    missing = [c for c in required if c not in pdf.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {', '.join(missing)}")

    work = pdf.copy()
    for col in [left_count_col, right_count_col, overlap_count_col]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
        if work[col].isna().any():
            raise ValueError(f"Column '{col}' contains non-numeric values")
        if (work[col] < 0).any():
            raise ValueError(f"Column '{col}' must be >= 0")

    if (work[overlap_count_col] > work[left_count_col]).any() or (work[overlap_count_col] > work[right_count_col]).any():
        raise ValueError("overlap_count cannot be greater than left_count/right_count")

    work["left_only"] = work[left_count_col] - work[overlap_count_col]
    work["right_only"] = work[right_count_col] - work[overlap_count_col]

    rows = []
    for _, r in work.iterrows():
        label = str(r[label_col])
        overlap = float(r[overlap_count_col])
        left_only = float(r["left_only"])
        right_only = float(r["right_only"])
        left_total = float(r[left_count_col])
        right_total = float(r[right_count_col])

        rows.append(
            {
                "row": f"{label} | {left_name}",
                "side": left_name,
                "overlap": overlap,
                "only": left_only,
                "total": left_total,
                "only_type": "left",
            }
        )
        rows.append(
            {
                "row": f"{label} | {right_name}",
                "side": right_name,
                "overlap": overlap,
                "only": right_only,
                "total": right_total,
                "only_type": "right",
            }
        )

    bars = pd.DataFrame(rows)

    fig = go.Figure()
    fig.add_bar(
        name="Overlap",
        y=bars["row"],
        x=bars["overlap"],
        orientation="h",
        marker_color=overlap_color,
        hovertemplate="<b>%{y}</b><br>Overlap: %{x:,.0f}<extra></extra>",
    )

    fig.add_bar(
        name=f"Only in {left_name}",
        y=bars["row"],
        x=bars.apply(lambda r: r["only"] if r["only_type"] == "left" else 0.0, axis=1),
        orientation="h",
        marker_color=left_only_color,
        hovertemplate="<b>%{y}</b><br>Only: %{x:,.0f}<extra></extra>",
    )

    fig.add_bar(
        name=f"Only in {right_name}",
        y=bars["row"],
        x=bars.apply(lambda r: r["only"] if r["only_type"] == "right" else 0.0, axis=1),
        orientation="h",
        marker_color=right_only_color,
        hovertemplate="<b>%{y}</b><br>Only: %{x:,.0f}<extra></extra>",
    )

    max_total = float(bars["total"].max()) if len(bars) > 0 else 1.0
    for _, r in bars.iterrows():
        overlap_pct = (r["overlap"] / r["total"] * 100) if r["total"] > 0 else 0.0
        fig.add_annotation(
            x=r["total"],
            y=r["row"],
            text=f"Total: {r['total']:,.0f} | Overlap: {overlap_pct:.1f}%",
            showarrow=False,
            xanchor="left",
            xshift=6,
            font=dict(size=11, color="#424242"),
        )

    fig.update_layout(
        title=f"<b>{title}</b>",
        barmode="stack",
        height=height,
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(size=12, family="Arial, sans-serif"),
        margin=dict(l=220, r=120, t=60, b=50),
        xaxis=dict(
            title="Count",
            showgrid=True,
            gridcolor="lightgray",
            range=[0, max_total * 1.25],
        ),
        yaxis=dict(
            showgrid=False,
            categoryorder="array",
            categoryarray=bars["row"].tolist(),
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        modebar=dict(
            remove=[
                "zoom2d",
                "pan2d",
                "select2d",
                "lasso2d",
                "autoScale2d",
                "resetScale2d",
                "toggleSpikelines",
                "hoverCompareCartesian",
                "hoverClosestCartesian",
            ]
        ),
    )

    if show_figure:
        fig.show(config={"displaylogo": False})

    return fig
