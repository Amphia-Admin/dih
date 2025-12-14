"""Config builder module."""

from dataclasses import dataclass
from typing import Any, cast

from loadcore.src.readers.constructors.secret_constructor import SecretProcessor


@dataclass(frozen=True)
class SystemConfiguration:
    """
    SystemConfiguration object class.

    Provides a structured way to access various configuration settings defined in
    a YAML file.
    """

    yaml_data: dict[str, Any]
    yaml_path: str

    @property
    def name(self) -> str:
        """
        Read-only property to access the name data from yaml.

        Returns
        -------
        str
            Name data from yaml

        """
        name = self.yaml_data.get("name")
        if not isinstance(name, str):
            err_msg = f"Expected 'name' to be a string, got {type(name)}"
            raise TypeError(err_msg)
        return name

    @property
    def local_settings_enabled(self) -> bool:
        """
        Read-only property to get the environment option from the yaml.

        Returns
        -------
        bool
            True or False local environment enabled

        """
        enabled = self.yaml_data.get("local_settings", {}).get("enabled")
        if isinstance(enabled, str):
            return enabled.lower() == "true"
        if not isinstance(enabled, bool):
            err_msg = f"Expected 'enabled' to be a bool, got {type(enabled)}"
            raise TypeError(err_msg)
        return enabled

    @property
    def local_catalog(self) -> str:
        """
        Read-only property to get the local catalog name.

        Returns
        -------
        str
            Local catalog name

        """
        catalog_name = self.yaml_data.get("local_settings", {}).get("catalog_name")
        if not isinstance(catalog_name, str):
            err_msg = (
                f"Expected 'catalog_name' to be a string, got {type(catalog_name)}"
            )
            raise TypeError(err_msg)
        return catalog_name

    @property
    def local_secrets(self) -> list[SecretProcessor]:
        """
        Read-only property to access the local secrets data from yaml.

        Returns
        -------
        list[SecretProcessor]
            List of constructed secrets data

        """
        secrets_data = self.yaml_data.get("local_secrets", [])
        if not isinstance(secrets_data, list):
            err_msg = f"Expected 'local_secrets' to be a list, got {type(secrets_data)}"
            raise TypeError(err_msg)

        if all(isinstance(secret, SecretProcessor) for secret in secrets_data):
            return secrets_data

        try:
            return [SecretProcessor(**secret) for secret in secrets_data]
        except TypeError as exc:
            err_msg = f"Failed to construct SecretProcessor: {exc}"
            raise TypeError(err_msg) from exc

    @property
    def remote_catalog(self) -> str:
        """
        Read-only property to get the remote catalog name.

        Returns
        -------
        str
            Remote catalog name

        """
        catalog_name = self.yaml_data.get("remote_settings", {}).get("catalog_name")
        if not isinstance(catalog_name, str):
            err_msg = (
                f"Expected 'catalog_name' to be a string, got {type(catalog_name)}"
            )
            raise TypeError(err_msg)
        return catalog_name

    @property
    def remote_secret_scope(self) -> str:
        """
        Read-only property to get the remote secret scope name.

        Returns
        -------
        str
            Remote secret scope name

        """
        secret_scope = self.yaml_data.get("remote_settings", {}).get("secret_scope")
        if not isinstance(secret_scope, str):
            err_msg = (
                f"Expected 'secret_scope' to be a string, got {type(secret_scope)}"
            )
            raise TypeError(err_msg)
        return secret_scope

    @property
    def logging(self) -> dict[str, Any]:
        """
        Read-only property to access the logging settings from yaml.

        Returns
        -------
        dict[str, Any]
            Logging settings data

        """
        logging_data = self.yaml_data.get("global", {}).get("logging", {})
        if not isinstance(logging_data, dict):
            err_msg = f"Expected 'logging' to be a dict, got {type(logging_data)}"
            raise TypeError(err_msg)
        return cast(dict[str, Any], logging_data)

    @property
    def validation(self) -> dict[str, Any]:
        """
        Read-only property to access the validation settings from yaml.

        Returns
        -------
        dict[str, Any]
            Validation settings data

        """
        validation_data = self.yaml_data.get("global", {}).get("validation", {})
        if not isinstance(validation_data, dict):
            err_msg = f"Expected 'validation' to be a dict, got {type(validation_data)}"
            raise TypeError(err_msg)
        return cast(dict[str, Any], validation_data)
