"""Secret management for loadcore.

Local secrets: Loaded from .env file by Docker.
Remote secrets: Loaded from Databricks Key Vault secret scope.
"""

from __future__ import annotations

import contextlib
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

with contextlib.suppress(ModuleNotFoundError):
    from pyspark.dbutils import DBUtils

logger = logging.getLogger(__name__)


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


def load_remote_secrets(spark: SparkSession, secret_scope: str) -> list[Secret]:
    """Load secrets from Databricks Key Vault secret scope.

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
    except (NameError, AttributeError, RuntimeError) as e:
        # NameError: DBUtils not available (local mode)
        # AttributeError: secrets API not available
        # RuntimeError: Databricks API errors
        logger.warning(f"Failed to load remote secrets: {e}")
        return []
    else:
        logger.info(f"Loaded {len(secrets)} secret(s) from scope '{secret_scope}'")
        return secrets


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
