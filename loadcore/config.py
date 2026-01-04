"""Configuration dataclasses for loadcore."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from src.core.types import PipelineMetadata, SparkConfig


class VolumeNotConfiguredError(KeyError):
    """Raised when a requested volume is not configured."""

    def __init__(self, volume_name: str) -> None:
        self.volume_name = volume_name
        super().__init__(f"Volume '{volume_name}' not configured in environment")


@dataclass(frozen=True)
class LocalEnvironmentConfig:
    """Configuration for local development environment."""

    catalog: str
    volumes: dict[str, str] = field(default_factory=dict)


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
    metadata: PipelineMetadata = field(default_factory=dict)
    static_config: SparkConfig = field(default_factory=dict)
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
        VolumeNotConfiguredError
            If volume name not found
        """
        if name not in self.volumes:
            raise VolumeNotConfiguredError(name)
        return self.volumes[name]
