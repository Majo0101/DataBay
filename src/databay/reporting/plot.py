from typing import Optional, Tuple, Union
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


def plot_match_percentage(
    df: Union[pd.DataFrame, 'DataFrame'],
    title: str = 'Column Match Percentage: PROD vs TEST',
    height: int = 600,
    column_col: str = 'column',
    match_col: str = 'match_%',
    show_figure: bool = False,
    x_range: Optional[Tuple[int, int]] = None,
    margin_left: int = 200
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
    
    # Set default x-axis range if not provided
    if x_range is None:
        x_range = (0, 115)
    
    # Create the bar chart
    fig = px.bar(
        df,
        y=column_col,
        x=match_col,
        orientation='h',
        text=match_col,
        color=match_col,
        color_continuous_scale=DATA_QUALITY_COLORS,
        labels={match_col: 'Match %', column_col: 'Column Name'},
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
            categoryarray=df[column_col].tolist(),
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
            **QUALITY_THRESHOLDS
        ),
        hoverlabel=dict(
            bgcolor="white",
            font_size=12,
            font_family="Arial, sans-serif"
        )
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
    # Convert to Pandas if needed
    df = _to_pandas(df)
    
    # Sort the data
    sorted_df = df.sort_values('match_%', ascending=ascending).copy()
    
    return plot_match_percentage(sorted_df, title=title, **kwargs)