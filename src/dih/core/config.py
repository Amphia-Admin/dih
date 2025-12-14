"""Configuration management for DIH framework.

This module provides configuration utilities for DIH pipelines, including
integration with loadcore for catalog management and secrets handling.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def create_run_config(
    root_path: str | Path,
    spark_conf: dict[str, str] | None = None,
    metadata: dict[str, Any] | None = None,
    static_config: dict[str, Any] | None = None,
    **kwargs: Any,  # noqa: ANN401
) -> dict[str, Any]:
    """
    Create a runtime configuration dictionary for pipeline execution.

    This follows the run_cfg pattern from pylynx, providing a simple
    dictionary-based configuration instead of class-based config.

    Parameters
    ----------
    root_path : str | Path
        Root path for the pipeline (lakehouse or Unity Catalog location).
    spark_conf : dict[str, str] | None, optional
        Spark configuration parameters (e.g., Delta optimizations).
    metadata : dict[str, Any] | None, optional
        Pipeline metadata for tracking and lineage.
    static_config : dict[str, Any] | None, optional
        Static configuration values for the pipeline.
    **kwargs : Any
        Additional configuration parameters.

    Returns
    -------
    dict[str, Any]
        Configuration dictionary ready for Runner initialization.

    Examples
    --------
    >>> # Local development with Spark
    >>> config = create_run_config(
    ...     root_path="/tmp/lakehouse",
    ...     spark_conf={"spark.sql.shuffle.partitions": "200"},
    ...     metadata={"pipeline": "orders"},
    ... )

    >>> # Databricks with Unity Catalog
    >>> config = create_run_config(
    ...     root_path="dbfs:/mnt/lakehouse",
    ...     metadata={"pipeline": "orders", "version": "1.0"},
    ... )

    """
    return {
        "root_path": str(root_path) if isinstance(root_path, Path) else root_path,
        "metadata": metadata or {},
        "static_config": static_config or {},
        "spark_conf": spark_conf or {},
        **kwargs,
    }
