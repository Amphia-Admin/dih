"""Schema application transforms for DataFrames."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType


def apply_schema(df: DataFrame, schema: StructType) -> DataFrame:
    """
    Apply a target schema to a DataFrame.

    Handles:
    - Missing columns: adds as NULL with correct type
    - Type mismatches: casts to target type
    - Extra columns: drops them (only keeps schema columns)
    - Column ordering: matches schema order

    Parameters
    ----------
    df : DataFrame
        Input DataFrame
    schema : StructType
        Target schema to apply

    Returns
    -------
    DataFrame
        DataFrame with schema applied
    """
    df_mapping = _ColumnMapping.from_schema(df.schema)
    target_mapping = _ColumnMapping.from_schema(schema)

    # Short-circuit if schemas match
    if df_mapping == target_mapping:
        return df

    missing = target_mapping - df_mapping
    to_cast = target_mapping.type_diff(df_mapping)

    select_cols = []
    for name, dtype in target_mapping.items():
        if name in missing:
            select_cols.append(lit(None).cast(dtype).alias(name))
        elif name in to_cast:
            select_cols.append(col(name).cast(dtype))
        else:
            select_cols.append(col(name))

    return df.select(*select_cols)


class _ColumnMapping(dict):
    """Internal mapping of lowercase column names to data types."""

    @classmethod
    def from_schema(cls, schema: StructType) -> _ColumnMapping:
        """Create mapping from StructType schema."""
        mapping = {}
        for field in schema.fields:
            lower_name = field.name.lower()
            if lower_name in mapping and field.dataType != mapping[lower_name]:
                msg = f"Ambiguous schema: duplicate column '{field.name}' with different types"
                raise ValueError(msg)
            mapping[lower_name] = field.dataType
        return cls(mapping)

    def __sub__(self, other: _ColumnMapping) -> dict:
        """Return columns in self but not in other."""
        return {k: v for k, v in self.items() if k not in other}

    def type_diff(self, other: _ColumnMapping) -> dict:
        """Return columns where types differ."""
        return {k: v for k, v in self.items() if k in other and v != other[k]}
