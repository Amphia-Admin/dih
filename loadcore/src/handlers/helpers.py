"""Helpers module."""

import os

from loadcore.src.handlers.error_exceptions import NoneError


def get_env_variable(env_value: str) -> str:
    """
    Instantiate an environment variable as object.

    Checks whether environment value exists.

    Parameters
    ----------
    env_value : str
        The environment variable name.

    Returns
    -------
    str
        The env variable assigned with value

    Raises
    ------
    NoneError
        Check if env variable exists

    """
    value = os.environ.get(env_value)

    if value is None:
        err_msg = f"Environment variable '{env_value}' not found."
        raise NoneError(err_msg)

    return value
