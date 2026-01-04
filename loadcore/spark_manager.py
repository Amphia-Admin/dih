"""Spark Manager module."""

__author__ = "Andreas"
__version__ = "1.0"
__project__ = "Colibri-Demo"

from abc import ABC, abstractmethod

from pyspark.sql import SparkSession


class AbstractSessionBuilder(ABC):
    """Abstract class that defines a Spark session."""

    @abstractmethod
    def create_spark_session(self) -> SparkSession:
        """Abstract function that creates a spark session."""


class LocalSparkSessionBuilder(AbstractSessionBuilder):
    """Provides methods to create and configure a Local Spark session."""

    def __init__(self, app_name: str, catalog_path: str) -> None:
        """
        Initialise a local spark session.

        Parameters
        ----------
        app_name : str
            The app name for the spark session constructor
        catalog_path : str
            Path to the catalog volume (used as Spark warehouse directory)

        """
        self.app_name = app_name
        self.catalog_path = catalog_path

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
            .config("spark.plugins", "io.dataflint.spark.SparkDataflintPlugin")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.warehouse.dir", self.catalog_path)
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        )

    def create_spark_session(self) -> SparkSession:
        """
        Create local spark session from builder using delta configuration.

        Returns
        -------
        SparkSession
            Create spark session

        """
        builder = self.builder.config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.13:4.0.0,io.dataflint:dataflint-spark4_2.13:0.7.0",
        )
        return builder.getOrCreate()


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
