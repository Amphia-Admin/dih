"""Secret constructor module."""

import os
import re

from yaml import SafeLoader, ScalarNode

from loadcore.src.handlers.error_exceptions import SecretLoaderExceptionError


class SecretProcessor:
    """
    Constructor Processor class that stores secrets.

    The scope and the secret are accessible through private read only properties.
    """

    def __init__(self, scope: str, secret: str) -> None:
        """
        Initialise the SecretProcessor with a scope and a secret.

        Parameters
        ----------
        scope : str
            The name of the secret scope
        secret : str
            The actual secret of the scope. Becomes hidden through setter
            read-only property

        """
        self.__scope = scope
        self.__secret = secret

    def __repr__(self) -> str:
        """Represent object ovewritten to hide the secret."""
        return f"<Secret(name={self.__scope}, secret=***)>"

    @property
    def scope(self) -> str:
        """Read only property to access the name/scope of the secret."""
        return self.__scope

    @property
    def secret(self) -> str:
        """Read only property to access the real secret."""
        return self.__secret


def add_secrets_constructor(loader: SafeLoader, node: ScalarNode) -> SecretProcessor:
    """
    Construct function which processes a YAML node.

    The constructor replaces placeholders with corresponding environment variable values
    The function searches for placeholders in the format ${TEST_SECRET} within
    the given node's value, and replaces them with the value of the environment
    variable 'TEST_SECRET'.

    Parameters
    ----------
    loader : SafeLoader
        The YAML Loader instance.
    node : ScalarNode
        The YAML Node being processed

    Returns
    -------
    SecretProcessor
        The SecretProcessor object to derive the secret properties from the constructor

    Raises
    ------
    SecretLoaderExceptionError
        Fail safe on environment variable identification

    """
    value = str(loader.construct_scalar(node))
    match = re.compile(r".*?\${([A-Za-z0-9_-]+)}.*?").findall(value)

    if match:
        for key in match:
            env_key = key.replace("-", "_")
            try:
                value = value.replace(f"${{{key}}}", os.environ[env_key])
            except KeyError as exc:
                raise SecretLoaderExceptionError(value) from exc

    return SecretProcessor(key, value)
