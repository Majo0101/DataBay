"""References to literal top-level Spark column names."""

from pyspark.sql import Column
from pyspark.sql import functions as F


def quote_identifier(name: str) -> str:
    """Quote a single identifier, including any embedded backticks."""
    return "`" + name.replace("`", "``") + "`"


def literal_col(name: str) -> Column:
    """Resolve the entire name as one column, rather than a nested field path."""
    return F.col(quote_identifier(name))
