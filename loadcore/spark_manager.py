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

    def __init__(self, app_name: str, catalog_name: str) -> None:
        """
        Initialise a local spark session.

        Parameters
        ----------
        app_name : str
            The app name for the spark session constructor
        catalog_name : str
            The local catalog name

        """
        self.app_name = app_name
        self.catalog_name = catalog_name

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
            .config("spark.sql.warehouse.dir", self.catalog_name)
            .config(
                f"spark.sql.catalog.{self.catalog_name}",  # Catalog cannot be renamed
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.databricks.delta.optimizeWrite.enabled", "true")
            .config("spark.databricks.delta.autoCompact.enabled", "true")
            # .config("spark.driver.memory", "4g")
            # .config("spark.executor.memory", "12g")
            # .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.execution.arrow.enabled", "true")
            # TODO: Set executor cores print(f"Executor cores: {sc.defaultParallelism}")
            # spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
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
