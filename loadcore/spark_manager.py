"""Spark Manager module."""

__author__ = "Andreas"
__version__ = "1.0"
__project__ = "Colibri-Demo"

from abc import ABC, abstractmethod

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


class AbstractSessionBuilder(ABC):
    """Abstract class that defines a Spark session."""

    @abstractmethod
    def create_spark_session(self) -> SparkSession:
        """Abstract function that creates a spark session."""


class LocalSparkSessionBuilder(AbstractSessionBuilder):
    """Provides methods to create and configure a Local Spark session."""

    def __init__(self, app_name: str, warehouse_path: str) -> None:
        """
        Initialise a local spark session.

        Parameters
        ----------
        app_name : str
            The app name for the spark session constructor
        warehouse_path : str
            Path to the local warehouse/catalog directory

        """
        self.app_name = app_name
        self.warehouse_path = warehouse_path

    @property
    def builder(self) -> SparkSession.Builder:
        """
        Create and return a Spark session configured with Delta Lake support.

        Returns
        -------
        SparkSession
            A Spark session object configured for Delta Lake operations.

        """
        return (
            SparkSession.builder.appName(self.app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.warehouse.dir", self.warehouse_path)
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .enableHiveSupport()
        )

    def create_spark_session(self) -> SparkSession:
        """
        Create local spark session from builder using delta configuration.

        Returns
        -------
        SparkSession
            Create spark session

        """
        return configure_spark_with_delta_pip(self.builder).getOrCreate()


class RemoteSparkSessionBuilder(AbstractSessionBuilder):
    """Provides methods to create and configure a Remote Spark session."""

    def create_spark_session(self) -> SparkSession:
        """
        Create remote spark session from builder.

        Returns
        -------
        SparkSession
            Get the active spark session of the remote cluster.

        """
        return SparkSession.getActiveSession()
