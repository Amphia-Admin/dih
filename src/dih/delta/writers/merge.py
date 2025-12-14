"""Delta merge writer using business keys."""

import logging
from typing import TYPE_CHECKING, Any

from delta.tables import DeltaTable

from src.dih.core.interfaces import TableDefinition, TargetTableDefMixin
from src.dih.delta.writers.base import DeltaWriterBase
from src.dih.delta.writers.utils import (
    build_column_mapping,
    build_merge_condition,
    get_merge_options,
    get_non_key_columns,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class DeltaMergeWriter(DeltaWriterBase):
    """
    Delta writer that performs merge operations using business keys.

    Merges data based on primary_keys from TableDefinition.
    Supports conditional updates, deletes, and custom column specifications.
    """

    def _merge(
        self,
        df: DataFrame,
        output_def: TableDefinition,
        spark: SparkSession,
        path: str,
        **kwargs: Any,
    ) -> None:
        """
        Perform merge operation using business keys.

        Parameters
        ----------
        df : DataFrame
            Source DataFrame to merge
        output_def : TableDefinition
            Target table definition
        spark : SparkSession
            SparkSession instance
        path : str
            Path to Delta table
        **kwargs : Any
            Additional merge options

        Raises
        ------
        ValueError
            If primary_keys are not defined
        """
        # Validate primary keys exist
        if not isinstance(output_def, TargetTableDefMixin):
            msg = "Output definition must implement TargetTableDefMixin for merge operations"
            raise TypeError(msg)

        primary_keys = output_def.primary_keys
        if not primary_keys:
            msg = (
                "primary_keys must be defined in TableDefinition for merge operations. "
                "Set the primary_keys property to a list of column names."
            )
            raise ValueError(msg)

        logger.info(f"Performing merge with primary keys: {primary_keys}")

        # Extract merge options with defaults
        merge_opts = get_merge_options(output_def)
        source_alias = merge_opts["source_alias"]
        target_alias = merge_opts["target_alias"]

        # Build merge condition from primary keys
        merge_condition = build_merge_condition(primary_keys, source_alias, target_alias)
        logger.debug(f"Merge condition: {merge_condition}")

        # Get all columns from source DataFrame
        all_columns = df.columns

        # Determine columns for update and insert
        columns_to_update = merge_opts["columns_to_update"]
        if columns_to_update is None:
            # Default: update all non-key columns
            columns_to_update = get_non_key_columns(all_columns, primary_keys)
        logger.debug(f"Columns to update: {columns_to_update}")

        columns_to_insert = merge_opts["columns_to_insert"]
        if columns_to_insert is None:
            # Default: insert all columns
            columns_to_insert = all_columns
        logger.debug(f"Columns to insert: {columns_to_insert}")

        # Build column mappings
        update_mapping = build_column_mapping(columns_to_update, source_alias)
        insert_mapping = build_column_mapping(columns_to_insert, source_alias)

        # Apply broadcast threshold if specified
        broadcast_threshold = merge_opts["broadcast_threshold"]
        if broadcast_threshold is not None:
            logger.info(f"Setting broadcast threshold: {broadcast_threshold}")
            spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(broadcast_threshold))

        # Load target Delta table
        target_delta_table = DeltaTable.forPath(spark, path)

        # Build merge builder
        merge_builder = target_delta_table.alias(target_alias).merge(
            df.alias(source_alias), merge_condition
        )

        # Add whenNotMatchedInsert clause
        when_not_matched_condition = merge_opts["when_not_matched_insert_condition"]
        if when_not_matched_condition:
            logger.debug(f"Using conditional insert: {when_not_matched_condition}")
            merge_builder = merge_builder.whenNotMatchedInsert(
                condition=when_not_matched_condition, values=insert_mapping
            )
        else:
            merge_builder = merge_builder.whenNotMatchedInsert(values=insert_mapping)

        # Add whenMatchedUpdate clause
        when_matched_update_condition = merge_opts["when_matched_update_condition"]
        if when_matched_update_condition:
            logger.debug(f"Using conditional update: {when_matched_update_condition}")
            merge_builder = merge_builder.whenMatchedUpdate(
                condition=when_matched_update_condition, set=update_mapping
            )
        else:
            merge_builder = merge_builder.whenMatchedUpdate(set=update_mapping)

        # Add whenMatchedDelete clause if specified
        when_matched_delete_condition = merge_opts["when_matched_delete_condition"]
        if when_matched_delete_condition:
            logger.info(f"Adding delete condition: {when_matched_delete_condition}")
            merge_builder = merge_builder.whenMatchedDelete(condition=when_matched_delete_condition)

        # Execute merge
        logger.info("Executing merge operation")
        merge_builder.execute()
        logger.info("Merge operation completed successfully")
