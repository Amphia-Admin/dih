"""Configs and Spark Session constructor."""

import contextlib
import os
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from loadcore.spark_manager import LocalSparkSessionBuilder, RemoteSparkSessionBuilder
from loadcore.src.configs.config_builder import SystemConfiguration
from loadcore.src.handlers.error_exceptions import (
    LocalConfigYamlSetupError,
    StrategyExceptionError,
)
from loadcore.src.readers.yaml_reader import yaml_extract

with contextlib.suppress(ModuleNotFoundError):
    from pyspark.dbutils import DBUtils


class AbstractConfigStrategy(ABC):
    """Abstract base class for configuration strategies."""

    def __init__(self, config_data: SystemConfiguration) -> None:
        """
        Initialise the configuration strategy with system configuration data.

        Parameters
        ----------
        config_data : SystemConfiguration
            The configuration data for the system.

        """
        self.config_data = config_data

    @property
    @abstractmethod
    def get_catalog_name(self) -> str:
        """Abstract function that requires the catalog_name from environment."""

    @abstractmethod
    def manage_secrets(self) -> None:
        """Abstract function that requires deploying the secrets from environment."""

    @abstractmethod
    def set_spark_session(self) -> SparkSession:
        """Abstract function that generates a spark session for local and remote."""

    @property
    def spark_home_exists(self) -> bool:
        """
        Check if SPARK_HOME is set in the environment.

        Returns
        -------
        bool
            Return spark_home flag

        """
        return "SPARK_HOME" in os.environ

    def validate_settings(self) -> None:
        """Validate configuration against the environment settings."""
        if self.spark_home_exists == bool(self.config_data.local_settings_enabled):
            raise LocalConfigYamlSetupError


class LocalConfigStrategy(AbstractConfigStrategy):
    """Strategy to handle local configuration settings and operations."""

    def __init__(
        self, config_data: SystemConfiguration, app_name: str, secret_yaml_path: str
    ) -> None:
        """
        Initialise local configuration strategy.

        Parameters
        ----------
        config_data : SystemConfiguration
            Configuration data including local settings
        app_name : str
            Name of the application.
        secret_yaml_path : str
            Path to the secret configuration YAML file

        """
        super().__init__(config_data)
        self.app_name = app_name
        self.secret_yaml_path = secret_yaml_path

    @property
    def get_catalog_name(self) -> str:
        """
        Abstract function that initialises the local catalog from config yaml.

        Returns
        -------
        str
            Return the local catalog name

        """
        return self.config_data.local_catalog

    def manage_secrets(self) -> None:
        """Abstract function that requires deploying the secrets from environment."""
        secret_config_data = yaml_extract(self.secret_yaml_path)
        for secret in secret_config_data.local_secrets:
            os.environ[secret.scope] = secret.secret
            print(f"Created environment variable for secret: {secret.scope}")  # Logging

    def set_spark_session(self) -> SparkSession:
        """
        Create and set the Spark session for local strategy.

        Returns
        -------
        SparkSession
            The configured Spark session.

        """
        return LocalSparkSessionBuilder(
            app_name=self.app_name, catalog_name=self.get_catalog_name
        ).create_spark_session()


class RemoteConfigStrategy(AbstractConfigStrategy):
    """Strategy to handle remote configuration settings and operations."""

    def __init__(self, config_data: SystemConfiguration) -> None:
        """
        Initialise remote configuration strategy.

        Parameters
        ----------
        config_data : SystemConfiguration
            Configuration data including remote settings.

        """
        super().__init__(config_data)

    @property
    def get_catalog_name(self) -> str:
        """
        Abstract function that initialises the remote catalog from config yaml.

        Returns
        -------
        str
            Return the remote catalog name in unity catalog

        """
        return self.config_data.remote_catalog

    def manage_secrets(self) -> None:
        """Abstract function that requires deploying the secrets from environment."""
        dbutils = DBUtils(self.set_spark_session())
        secret_scope = self.config_data.remote_secret_scope
        secrets_list = dbutils.secrets.list(secret_scope)
        for secret in secrets_list:
            secret_value = dbutils.secrets.get(secret_scope, secret.key)
            os.environ[secret.key] = secret_value
            print(f"Created environment variable for secret: {secret.key}")  # Logging

    def set_spark_session(self) -> SparkSession:
        """
        Create and set the Spark session for remote.

        Returns
        -------
        SparkSession
            The configured Spark session.

        """
        return RemoteSparkSessionBuilder().create_spark_session()


class Configuration:
    """
    Main configuration interface.

    Initialises and executes strategy based configurations.
    """

    def __init__(self) -> None:
        """Initialise configuration with paths and load settings."""
        self.config_yaml_path = "loadcore/system/configs/local.config.yaml"
        self.secret_yaml_path = "loadcore/system/configs/secrets.config.yaml"  # noqa: S105 #TODO: Move this, local only.
        self.config_data = yaml_extract(self.config_yaml_path)
        self.app_name = "demo"
        self.local_enabled = self.config_data.local_settings_enabled

    def __initialise_strategy(self) -> AbstractConfigStrategy:
        """
        Initialise strategy based on configuration settings.

        Returns
        -------
        ConfigStrateg
            The initialised strategy.

        Raises
        ------
        StrategyExceptionError
            Invalid strategy configuration from the local yaml

        """
        strategy: AbstractConfigStrategy

        match self.local_enabled:
            case True:
                strategy = LocalConfigStrategy(
                    config_data=self.config_data,
                    app_name=self.app_name,
                    secret_yaml_path=self.secret_yaml_path,
                )
            case False:
                strategy = RemoteConfigStrategy(config_data=self.config_data)
            case _:
                raise StrategyExceptionError(self.local_enabled)
        return strategy

    def execute(self) -> tuple[SparkSession, str]:
        """
        Interface to execute local or remote strategy.

        Returns
        -------
        tuple[SparkSession, str]
            The sparksession and the catalog name

        """
        strategy = self.__initialise_strategy()
        strategy.validate_settings()
        strategy.manage_secrets()
        print(f"{strategy} initialised.")  # TODO: Replace with Logging
        return strategy.set_spark_session(), strategy.get_catalog_name
