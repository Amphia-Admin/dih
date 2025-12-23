"""Environment detection and initialisation."""

from __future__ import annotations

import atexit
import logging
import logging.config
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import SparkSession

from loadcore.config import (
    LocalEnvironmentConfig,
    PipelineConfig,
    RemoteEnvironmentConfig,
)
from loadcore.secrets import (
    inject_secrets_to_env,
    load_local_secrets,
    load_remote_secrets,
)
from loadcore.spark_manager import LocalSparkSessionBuilder, RemoteSparkSessionBuilder

logger = logging.getLogger(__name__)


class ConfigLoadError(Exception):
    """Cannot load configuration."""

    def __init__(self, path: Path, reason: str) -> None:
        super().__init__(f"Cannot load config from '{path}': {reason}")


class Environment:
    """Manages environment detection and initialisation.

    Auto-detects whether running locally or on Databricks, loads the
    appropriate configuration section, and initialises Spark session
    and secrets.

    Parameters
    ----------
    config_path : Path | str
        Path to the environment config YAML file.
        Defaults to "env.config.yaml" in the current directory.

    Examples
    --------
    Basic usage:

        env = Environment("./env.config.yaml")
        spark, catalog = env.initialise()

    With PipelineConfig:

        env = Environment()
        config = env.for_pipeline(
            metadata={"name": "orders", "version": "1.0"},
        )
        Runner(config=config, pipeline=MyPipeline).run()
    """

    def __init__(self, config_path: Path | str = "env.config.yaml") -> None:
        self._config_path = Path(config_path)
        self._mode: str | None = None
        self._spark: SparkSession | None = None
        self._catalog: str | None = None
        self._volumes: dict[str, str] = {}
        self._initialised = False
        self._logging_initialised = False

    @property
    def mode(self) -> str:
        """Get the detected environment mode ('local' or 'remote')."""
        if self._mode is None:
            self._mode = self._detect_mode()
        return self._mode

    @property
    def is_local(self) -> bool:
        """Check if running in local mode."""
        return self.mode == "local"

    @property
    def is_remote(self) -> bool:
        """Check if running in remote mode."""
        return self.mode == "remote"

    @property
    def volumes(self) -> dict[str, str]:
        """Get the volume mappings for current environment."""
        return self._volumes

    def _detect_mode(self) -> str:
        """Auto-detect environment mode.

        Returns 'remote' if there's an active SparkSession (Databricks),
        otherwise 'local' (we'll create a session).
        """
        if SparkSession.getActiveSession() is not None:
            logger.info("Detected remote environment (active SparkSession)")
            return "remote"

        logger.info("Detected local environment (no active SparkSession)")
        return "local"

    def _load_yaml_config(self) -> dict[str, Any]:
        """Load the YAML configuration file."""
        if not self._config_path.exists():
            raise ConfigLoadError(self._config_path, "file not found")

        try:
            with self._config_path.open() as f:
                return yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigLoadError(self._config_path, str(e)) from e

    def _get_local_config(self, data: dict[str, Any]) -> LocalEnvironmentConfig:
        """Extract local configuration from YAML data."""
        local = data.get("local", {})
        return LocalEnvironmentConfig(
            catalog=local.get("catalog", "spark_catalog"),
            warehouse_path=local.get("warehouse_path", "./data/catalog"),
            volumes=local.get("volumes", {}),
            secrets_path=local.get("secrets_path"),
        )

    def _get_remote_config(self, data: dict[str, Any]) -> RemoteEnvironmentConfig:
        """Extract remote configuration from YAML data."""
        remote = data.get("remote", {})
        return RemoteEnvironmentConfig(
            catalog=remote.get("catalog", "default"),
            volumes=remote.get("volumes", {}),
            secret_scope=remote.get("secret_scope", ""),
        )

    def _create_spark_session(self, warehouse_path: str | None = None) -> SparkSession:
        """Create or get Spark session based on mode."""
        if self.is_local:
            return LocalSparkSessionBuilder(
                app_name="dih",
                warehouse_path=warehouse_path or "./data/catalog",
            ).create_spark_session()
        else:
            return RemoteSparkSessionBuilder().create_spark_session()

    def _setup_logging(self) -> None:
        """Initialise logging from custom_logger config."""
        if self._logging_initialised:
            return

        config_file = Path("custom_logger/config.yaml")
        if config_file.exists():
            config = yaml.safe_load(config_file.read_text())
            logging.config.dictConfig(config)

            queue_handler = logging.getHandlerByName("queue_handler")
            if queue_handler is not None:
                queue_handler.listener.start()
                atexit.register(queue_handler.listener.stop)

        self._logging_initialised = True
        logger.info("Logging initialised")

    def initialise(self) -> tuple[SparkSession, str]:
        """Initialise the environment.
        Detects mode, loads configuration, initialises logging, secrets,
        and creates/gets Spark session.

        Returns
        -------
        tuple[SparkSession, str]
            The Spark session and catalog name
        """
        if self._initialised:
            return self._spark, self._catalog

        self._setup_logging()

        yaml_data = self._load_yaml_config()

        if self.is_local:
            config = self._get_local_config(yaml_data)
            self._catalog = config.catalog
            self._volumes = config.volumes

            if config.secrets_path:
                secrets_path = self._config_path.parent / config.secrets_path
                secrets = load_local_secrets(secrets_path)
                inject_secrets_to_env(secrets)

            self._spark = self._create_spark_session(config.warehouse_path)

        else:
            config = self._get_remote_config(yaml_data)
            self._catalog = config.catalog
            self._volumes = config.volumes

            self._spark = self._create_spark_session()

            if config.secret_scope:
                secrets = load_remote_secrets(self._spark, config.secret_scope)
                inject_secrets_to_env(secrets)

        self._initialised = True
        logger.info(f"Initialised: mode={self.mode}, catalog={self._catalog}")

        return self._spark, self._catalog

    def for_pipeline(
        self,
        metadata: dict[str, Any] | None = None,
        static_config: dict[str, Any] | None = None,
        spark_conf: dict[str, str] | None = None,
    ) -> PipelineConfig:
        """Create a PipelineConfig for running a pipeline.

        Initialises the environment if not already done.

        Parameters
        ----------
        metadata : dict[str, Any] | None
            Pipeline metadata
        static_config : dict[str, Any] | None
            Static configuration to inject into pipeline
        spark_conf : dict[str, str] | None
            Spark configuration overrides

        Returns
        -------
        PipelineConfig
            Configuration ready for the Runner
        """
        spark, catalog = self.initialise()

        return PipelineConfig(
            spark=spark,
            catalog=catalog,
            volumes=self._volumes,
            metadata=metadata or {},
            static_config=static_config or {},
            spark_conf=spark_conf or {},
        )
