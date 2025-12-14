"""Path utilities for lakehouse operations."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dih.constants import LakeLayer

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class PathBuilder:
    """Fluent API for constructing lakehouse paths."""

    def __init__(self, root_path: str | Path) -> None:
        """
        Initialize with root path.

        Parameters
        ----------
        root_path : str | Path
            Base path for all data (local, S3, DBFS, etc.)
        """
        self._root = Path(root_path) if isinstance(root_path, str) else root_path
        self._layer: str | None = None
        self._table: str | None = None
        self._partitions: dict[str, Any] = {}

    def layer(self, layer: LakeLayer | str) -> PathBuilder:
        """
        Set lakehouse layer.

        Parameters
        ----------
        layer : LakeLayer | str
            LakeLayer enum or string (bronze, silver, gold, landing)

        Returns
        -------
        PathBuilder
            Self for method chaining
        """
        if isinstance(layer, LakeLayer):
            self._layer = layer.value
        else:
            self._layer = layer.lower().strip("/")
        return self

    def table(self, name: str) -> PathBuilder:
        """
        Set table/dataset name.

        Parameters
        ----------
        name : str
            Table or dataset name

        Returns
        -------
        PathBuilder
            Self for method chaining
        """
        self._table = name.strip("/")
        return self

    def partition(self, **kwargs: Any) -> PathBuilder:
        """
        Add partition components.

        Parameters
        ----------
        **kwargs : Any
            Partition key-value pairs (e.g., year=2024, month=1)

        Returns
        -------
        PathBuilder
            Self for method chaining
        """
        self._partitions.update(kwargs)
        return self

    def build(self) -> str:
        """
        Construct final path string.

        Returns
        -------
        str
            Complete path as string

        Raises
        ------
        ValueError
            If layer is not specified
        """
        if self._layer is None:
            msg = "Layer must be specified via .layer() method"
            raise ValueError(msg)

        # Build path components
        parts = [str(self._root), self._layer]

        if self._table:
            parts.append(self._table)

        # Add partitions in key=value format
        for key, value in self._partitions.items():
            parts.append(f"{key}={value}")

        # Join and normalize
        path = "/".join(parts)
        # Remove double slashes and trailing slash
        path = path.replace("//", "/").rstrip("/")

        logger.debug(f"Built path: {path}")
        return path

    def as_path(self) -> Path:
        """
        Construct final Path object.

        Returns
        -------
        Path
            Complete path as Path object
        """
        return Path(self.build())


def path_exists(path: str | Path, spark: SparkSession | None = None) -> bool:
    """
    Check if path exists (supports local, DBFS, S3).

    Parameters
    ----------
    path : str | Path
        Path to check
    spark : SparkSession | None, optional
        Optional SparkSession for distributed filesystem checks

    Returns
    -------
    bool
        True if path exists and is accessible
    """
    path_str = str(path)

    # For distributed filesystems (S3, DBFS), use Spark if available
    if spark and (path_str.startswith(("s3://", "s3a://", "dbfs://", "abfss://"))):
        try:
            # Access Spark's JVM for Hadoop filesystem operations
            # Using private Spark attributes for Hadoop FileSystem access
            fs_class = spark._jvm.org.apache.hadoop.fs.FileSystem  # type: ignore
            hadoop_path_class = spark._jvm.org.apache.hadoop.fs.Path  # type: ignore
            hadoop_conf = spark._jsc.hadoopConfiguration()

            # Check if path exists using Hadoop FileSystem
            fs = fs_class.get(hadoop_conf)
            hadoop_path = hadoop_path_class(path_str)
            return bool(fs.exists(hadoop_path))
        except Exception as e:
            logger.debug(f"Path check failed for {path_str}: {e}")
            return False

    # For local paths
    return Path(path).exists()
