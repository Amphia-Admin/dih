"""Error exceptions module."""


class LoaderExceptionError(Exception):
    """Yaml loader exception type."""


class SecretLoaderExceptionError(LoaderExceptionError):
    """Yaml Secret loader exception type."""

    def __init__(self, value: str) -> None:
        """
        Initialise secretloader exception.

        Parameters
        ----------
        value : str
            The secret value.

        """
        super().__init__(
            f"Environment variable error. Cannot find {value} in environment variables."
        )


class StrategyExceptionError(Exception):
    """Strategy exception type."""

    def __init__(self, value: str) -> None:
        """
        Initialise strategy exception.

        Parameters
        ----------
        value : str
            The strategy type.

        """
        super().__init__(f"Invalid environment '{value}' initialised.")


class LocalConfigYamlSetupError(Exception):
    """Inproper config setup."""

    def __init__(self) -> None:
        """Initialise exception."""
        super().__init__(
            "Mismatch in environment setup! Please review configuration yaml."
        )


class NoneError(Exception):
    """NoneType error."""
