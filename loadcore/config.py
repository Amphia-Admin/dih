"""Configuration dataclasses for loadcore."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class LocalEnvironmentConfig:
    """Configuration for local development environment."""

    catalog: str
    warehouse_path: str
    volumes: dict[str, str] = field(default_factory=dict)
    secrets_path: str | None = None


@dataclass(frozen=True)
class RemoteEnvironmentConfig:
    """Configuration for remote Databricks environment."""

    catalog: str
    volumes: dict[str, str] = field(default_factory=dict)
    secret_scope: str = ""


@dataclass
class PipelineConfig:
    """Configuration for running a ih pipeline.

    This is provided by the user at runtime for each pipeline execution.
    """

    spark: SparkSession
    catalog: str
    volumes: dict[str, str] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    static_config: dict[str, Any] = field(default_factory=dict)
    spark_conf: dict[str, str] = field(default_factory=dict)

    def get_volume(self, name: str) -> str:
        """Get a volume path by name.

        Parameters
        ----------
        name : str
            Volume name (e.g., 'lake')

        Returns
        -------
        str
            The volume path for current environment

        Raises
        ------
        KeyError
            If volume name not found
        """
        if name not in self.volumes:
            raise KeyError(f"Volume '{name}' not configured in environment")
        return self.volumes[name]
