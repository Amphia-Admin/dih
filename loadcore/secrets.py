"""Secret management for loadcore."""

from __future__ import annotations

import contextlib
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

with contextlib.suppress(ModuleNotFoundError):
    from pyspark.dbutils import DBUtils

logger = logging.getLogger(__name__)


class SecretLoadError(Exception):
    """Error loading a secret."""

    def __init__(self, key: str) -> None:
        super().__init__(f"Cannot resolve secret: '{key}' not found in environment.")


@dataclass(frozen=True)
class Secret:
    """Container for a secret with hidden repr."""

    name: str
    _value: str

    def __repr__(self) -> str:
        """Return string representation with hidden value."""
        return f"Secret(name={self.name!r}, value=***)"

    @property
    def value(self) -> str:
        """Access the secret value."""
        return self._value


def _resolve_env_placeholder(value: str) -> tuple[str, str]:
    """Resolve ${VAR_NAME} placeholder from environment.

    Returns
    -------
    tuple[str, str]
        (variable_name, resolved_value)
    """
    pattern = re.compile(r"\${([A-Za-z0-9_-]+)}")
    match = pattern.search(value)

    if not match:
        return ("unknown", value)

    key = match.group(1)
    env_key = key.replace("-", "_")

    try:
        resolved = os.environ[env_key]
        return (key, resolved)
    except KeyError:
        raise SecretLoadError(key) from None


def _yaml_secret_constructor(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> Secret:
    """YAML constructor for !secret tags."""
    raw_value = str(loader.construct_scalar(node))
    name, resolved = _resolve_env_placeholder(raw_value)
    return Secret(name=name, _value=resolved)


class SecretLoader(yaml.SafeLoader):
    """YAML loader with secret tag support."""

    pass


SecretLoader.add_constructor("!secret", _yaml_secret_constructor)


def load_local_secrets(secrets_path: Path | str) -> list[Secret]:
    """Load secrets from a local YAML file.

    The YAML file should have a `local_secrets` key with a list of
    !secret tags that reference environment variables.

    Parameters
    ----------
    secrets_path : Path | str
        Path to the secrets YAML file

    Returns
    -------
    list[Secret]
        List of loaded secrets
    """
    path = Path(secrets_path)

    if not path.exists():
        logger.debug(f"Secrets file not found: {path}")
        return []

    with open(path) as f:
        data = yaml.load(f, Loader=SecretLoader)

    secrets = data.get("local_secrets", [])

    if not isinstance(secrets, list):
        return []

    # Filter to only Secret instances (in case of parsing issues)
    return [s for s in secrets if isinstance(s, Secret)]


def load_remote_secrets(spark: SparkSession, secret_scope: str) -> list[Secret]:
    """Load secrets from Databricks Key Vault scope.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for DBUtils
    secret_scope : str
        Name of the Databricks secret scope

    Returns
    -------
    list[Secret]
        List of loaded secrets
    """
    if not secret_scope:
        logger.debug("No secret scope configured")
        return []

    try:
        dbutils = DBUtils(spark)
        secrets_list = dbutils.secrets.list(secret_scope)

        secrets = []
        for secret_metadata in secrets_list:
            value = dbutils.secrets.get(secret_scope, secret_metadata.key)
            secrets.append(Secret(name=secret_metadata.key, _value=value))

        return secrets
    except Exception as e:
        logger.warning(f"Failed to load remote secrets: {e}")
        return []


def inject_secrets_to_env(secrets: list[Secret]) -> None:
    """Inject secrets as environment variables.

    Parameters
    ----------
    secrets : list[Secret]
        Secrets to inject
    """
    for secret in secrets:
        os.environ[secret.name] = secret.value
        logger.debug(f"Injected secret: {secret.name}")

    if secrets:
        logger.info(f"Injected {len(secrets)} secret(s) into environment")
