import pytest
import pandas as pd

from databay.reporting.plot import (
    dataframe_to_dict,
    plot_match_percentage,
    plot_match_percentage_sorted,
    plot_overlap,
)


def test_plot_match_percentage_builds_figure_from_dict():
    fig = plot_match_percentage(
        {"customer_id": 100.0, "email": 97.4, "status": 62.0},
        sort=False,
    )

    assert len(fig.data) == 1
    assert fig.data[0].type == "bar"
    assert list(fig.data[0].y) == ["customer_id", "email", "status"]


def test_plot_match_percentage_sorted_orders_values():
    fig = plot_match_percentage_sorted(
        {"a": 80.0, "b": 99.0, "c": 70.0},
        ascending=True,
    )

    assert list(fig.data[0].y) == ["c", "a", "b"]


def test_plot_match_percentage_rejects_empty_input():
    with pytest.raises(ValueError, match="must not be empty"):
        plot_match_percentage({})


def test_plot_match_percentage_rejects_non_finite_values():
    with pytest.raises(ValueError, match="finite number"):
        plot_match_percentage({"a": float("inf")})


def test_plot_overlap_builds_expected_traces():
    fig = plot_overlap(
        {"left_count": 120, "right_count": 100, "overlap_count": 90},
        left_name="Dataset A",
        right_name="Dataset B",
    )

    assert len(fig.data) == 3
    assert [t.name for t in fig.data] == ["Overlap", "Only in Dataset A", "Only in Dataset B"]
    assert fig.layout.barmode == "stack"


def test_plot_overlap_rejects_missing_keys():
    with pytest.raises(ValueError, match="Missing required key"):
        plot_overlap({"left_count": 10, "right_count": 8})


def test_plot_overlap_rejects_invalid_overlap():
    with pytest.raises(ValueError, match="cannot be greater"):
        plot_overlap({"left_count": 10, "right_count": 8, "overlap_count": 11})


def test_dataframe_to_dict_builds_mapping():
    df = pd.DataFrame({"column": ["a", "b"], "match_%": [99.0, 87.5]})
    out = dataframe_to_dict(df, key_col="column", value_col="match_%")
    assert out == {"a": 99.0, "b": 87.5}


def test_dataframe_to_dict_duplicate_policies():
    df = pd.DataFrame({"column": ["a", "a", "b"], "match_%": [10.0, 20.0, 30.0]})

    with pytest.raises(ValueError, match="Duplicate keys"):
        dataframe_to_dict(df, key_col="column", value_col="match_%")

    first_out = dataframe_to_dict(df, key_col="column", value_col="match_%", duplicate_policy="first")
    last_out = dataframe_to_dict(df, key_col="column", value_col="match_%", duplicate_policy="last")

    assert first_out == {"a": 10.0, "b": 30.0}
    assert last_out == {"a": 20.0, "b": 30.0}
