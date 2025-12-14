"""Configuration for orders pipeline.

This module contains configuration settings for the orders data pipeline,
including paths, Spark settings, and pipeline execution parameters.
"""

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class PipelineConfig:
    """
    Configuration container for the orders pipeline.

    Attributes
    ----------
    pipeline_root : Path
        Root directory of the orders pipeline (examples/pipelines/orders).
    lakehouse_root : Path
        Root directory for Delta Lake tables (lakehouse output).
    data_dir : Path
        Directory containing source CSV files.

    """

    def __init__(self, lakehouse_root: Path | None = None) -> None:
        """
        Initialize pipeline configuration.

        Parameters
        ----------
        lakehouse_root : Path | None, optional
            Root directory for lakehouse output. If None, defaults to
            pipeline_root/lakehouse directory.

        """
        # Pipeline root is the directory containing this config file
        self.pipeline_root = Path(__file__).parent

        # Lakehouse root is where Delta tables are written
        if lakehouse_root is None:
            self.lakehouse_root = self.pipeline_root / "lakehouse"
        else:
            self.lakehouse_root = lakehouse_root

        # Data directory contains source CSV files
        self.data_dir = self.pipeline_root / "data"

    def get_run_config(self) -> dict[str, str]:
        """
        Get configuration dictionary for DIH Runner.

        Returns
        -------
        dict[str, str]
            Configuration dictionary with root_path for Runner initialization.

        """
        return {
            "root_path": str(self.pipeline_root),
        }


def configure_spark_with_delta_pip(
    builder: SparkSession.Builder,
) -> SparkSession.Builder:
    """
    Configure Spark session builder with Delta Lake support using delta-spark package.

    This function adds the necessary Delta Lake packages and configurations
    to the Spark session builder for use with pip-installed delta-spark.

    Parameters
    ----------
    builder : SparkSession.Builder
        Spark session builder to configure.

    Returns
    -------
    SparkSession.Builder
        Configured Spark session builder with Delta Lake support.

    Notes
    -----
    This configuration assumes delta-spark is installed via pip.
    For production deployments, consider using Maven coordinates instead.

    Examples
    --------
    >>> from pyspark.sql import SparkSession
    >>> builder = SparkSession.builder.appName("MyApp")
    >>> builder = configure_spark_with_delta_pip(builder)
    >>> spark = builder.getOrCreate()

    """
    return builder.config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.0.0",
    )
