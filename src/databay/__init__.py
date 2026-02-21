"""
DataBay - Data quality and comparison tooling for lakehouse datasets
"""

# Import core functionality directly at package level
from databay.core.metrics import (
    compare_datasets,
    compare_columns_by_key,
    compare_schema,
    numeric_diff_check,
)

from databay.core.quality import (
    null_rate,
    pk_uniqueness_check,
    duplicate_check,
)

# Make subpackages available
from databay import core
from databay import etl
from databay import reporting
from databay import runtime

__version__ = "0.1.0"

__all__ = [
    # Core metrics functions
    "compare_datasets",
    "compare_columns_by_key",
    "compare_schema",
    "numeric_diff_check",
    # Core quality functions
    "null_rate",
    "pk_uniqueness_check",
    "duplicate_check",
    # Subpackages
    "core",
    "etl",
    "reporting",
    "runtime",
]
