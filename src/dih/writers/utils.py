"""Utility functions for Delta merge operations."""

import logging
from typing import TYPE_CHECKING, Any

from src.dih.core.table_interfaces import TableDefinition, TargetTableDefMixin

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def build_merge_condition(
    columns: list[str],
    source_alias: str = "src",
    target_alias: str = "tgt",
) -> str:
    """
    Build SQL merge condition from column list.

    Generates an AND-joined predicate for matching records between
    source and target tables during Delta merge operations.

    Parameters
    ----------
    columns : list[str]
        List of column names to include in merge condition
    source_alias : str, optional
        Alias for source table (default: "src")
    target_alias : str, optional
        Alias for target table (default: "tgt")

    Returns
    -------
    str
        SQL predicate string for merge condition

    Examples
    --------
    >>> build_merge_condition(["id", "product_id"], "s", "t")
    "s.id = t.id and s.product_id = t.product_id"
    """
    if not columns:
        msg = "At least one column required for merge condition"
        raise ValueError(msg)

    predicates = [f"{source_alias}.{col} = {target_alias}.{col}" for col in columns]
    condition = " and ".join(predicates)

    logger.debug(f"Built merge condition: {condition}")
    return condition


def build_column_mapping(
    columns: list[str],
    source_alias: str = "src",
) -> dict[str, str]:
    """
    Build column mapping for insert/update operations.

    Creates a dictionary mapping target columns to source columns with alias.

    Parameters
    ----------
    columns : list[str]
        List of column names
    source_alias : str, optional
        Alias for source table (default: "src")

    Returns
    -------
    dict[str, str]
        Dictionary mapping column names to aliased source columns

    Examples
    --------
    >>> build_column_mapping(["id", "name", "status"], "s")
    {"id": "s.id", "name": "s.name", "status": "s.status"}
    """
    return {col: f"{source_alias}.{col}" for col in columns}


def build_auto_partition_predicate(
    df: DataFrame,
    partition_columns: list[str],
    target_alias: str = "tgt",
) -> str:
    """
    Build partition predicate from DataFrame's distinct partition values.

    Automatically generates partition lock predicates by extracting distinct
    values from the source DataFrame for each partition column.

    Parameters
    ----------
    df : DataFrame
        Source DataFrame containing data to merge
    partition_columns : list[str]
        List of partition column names
    target_alias : str, optional
        Alias for target table (default: "tgt")

    Returns
    -------
    str
        SQL predicate string for partition locking

    Examples
    --------
    If df has year=[2024, 2025] and month=[1, 12]:
    Returns: "(tgt.year in (2024, 2025)) and (tgt.month in (1, 12))"
    """
    if not partition_columns:
        msg = "At least one partition column required"
        raise ValueError(msg)

    predicates = []

    for col in partition_columns:
        # Get distinct values for this partition column
        distinct_values = df.select(col).distinct().collect()

        if not distinct_values:
            msg = f"No values found for partition column '{col}'"
            raise ValueError(msg)

        # Format values (handle strings vs numbers)
        values_list = []
        for row in distinct_values:
            value = getattr(row, col)
            if value is None:
                continue
            if isinstance(value, str):
                # Escape single quotes in strings
                escaped = value.replace("'", "''")
                values_list.append(f"'{escaped}'")
            else:
                values_list.append(str(value))

        if not values_list:
            msg = f"All values are NULL for partition column '{col}'"
            raise ValueError(msg)

        values_str = ", ".join(values_list)
        predicates.append(f"({target_alias}.{col} in ({values_str}))")

    predicate = " and ".join(predicates)
    logger.debug(f"Built partition predicate: {predicate}")

    return predicate


def get_non_key_columns(all_columns: list[str], key_columns: list[str]) -> list[str]:
    """
    Get columns that are not in the key columns list.

    Parameters
    ----------
    all_columns : list[str]
        List of all column names
    key_columns : list[str]
        List of key column names to exclude

    Returns
    -------
    list[str]
        List of non-key column names

    Examples
    --------
    >>> get_non_key_columns(["id", "name", "status"], ["id"])
    ["name", "status"]
    """
    return [col for col in all_columns if col not in key_columns]


def get_merge_options(output_def: TableDefinition) -> dict[str, Any]:
    """
    Extract merge options from TableDefinition with defaults.

    Returns a dictionary with default values for all merge options.
    If output_def has merge_options property, those values override defaults.

    Parameters
    ----------
    output_def : TableDefinition
        TableDefinition instance

    Returns
    -------
    dict[str, Any]
        Dictionary with all merge option keys and values

    Examples
    --------
    >>> opts = get_merge_options(table_def)
    >>> opts["source_alias"]
    "src"
    """
    defaults: dict[str, Any] = {
        "source_alias": "src",
        "target_alias": "tgt",
        "when_matched_update_condition": None,
        "when_matched_delete_condition": None,
        "when_not_matched_insert_condition": None,
        "columns_to_update": None,
        "columns_to_insert": None,
        "broadcast_threshold": None,
    }

    if isinstance(output_def, TargetTableDefMixin) and (merge_options := output_def.merge_options):
        defaults.update(merge_options)

    return defaults
