"""Yaml reader module."""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from types import TracebackType
from typing import IO

import yaml

from loadcore.src.configs.config_builder import SystemConfiguration
from loadcore.src.readers.constructors.secret_constructor import add_secrets_constructor

DataDict = dict[str, dict[str, str]]  # TODO: Change this to a refactored dataclass
_ReadStream = IO[str] | IO[bytes]


class SuperLoader(yaml.SafeLoader):
    """
    A custom YAML loader.

    Allows for the addition of custom constructors
    to handle specific tags in the YAML content.
    """

    def __init__(self, stream: _ReadStream) -> None:
        """
        Initialise the SuperLoader with the provided stream.

        Parameters
        ----------
        stream : _type_
            The stream of the YAML file being loaded

        """
        self._root = os.path.dirname(stream.name)
        super().__init__(stream)


class AbstractYamlReader(ABC):
    """Abstract base class for YAML readers."""

    @property
    @abstractmethod
    def yaml_path(self) -> str:
        """Abstract property to get the YAML file path."""

    @property
    @abstractmethod
    def data(self) -> DataDict:
        """Abstract property to get the processed data from the YAML file."""

    @abstractmethod
    def read(self) -> DataDict:
        """
        Abstract method to read and process the YAML file.

        Should be accessible from the context manager.
        """


class YamlReader(AbstractYamlReader):
    """
    Custom implementation of the AbstractYamlReader.

    Purpose is reading YAML files, and adding custom constructors.
    It uses a customer loader instance so that the streams are not overwritten

    Parameters
    ----------
    AbstractYamlReader : ABC
        Inherits abstract properties from collection.abc class of AbstractYamlReader.

    """

    def __init__(self, yaml_path: str, loader_instance: type[yaml.SafeLoader]) -> None:
        """
        Initialise a YamlReader instance with the specified YAML file path.

        Parameters
        ----------
        yaml_path : str
            The path to the YAML file to be read
        loader_instance : type[yaml.SafeLoader]
            yaml.SafeLoader or inherited classes of the loader instance

        """
        self._yaml_path = yaml_path
        self.loader_instance = loader_instance
        self._data: DataDict = {}

    def __enter__(self) -> DataDict:
        """
        With statement rquires enter to use class with context manager.

        Returns
        -------
        The read data from the provided YAML file

        """
        return self.read()

    def __exit__(
        self,
        typ: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """With statement requires safe exit from context manager."""

    def read(self) -> DataDict:
        """
        Read and process the YAML file using a custom loader instance.

        Returns
        -------
        DataDict
            The processed data from the YAML file

        """
        # Add more constructors here if needed.
        self.loader_instance.add_constructor("!secret", add_secrets_constructor)

        self._data = yaml.load(
            stream=open(self._yaml_path, "rb"),  # noqa: PTH123, SIM115 fmt: skip
            Loader=self.loader_instance,
        )

        return self._data

    @property
    def yaml_path(self) -> str:
        """
        Gets and sets the YAML file path.

        Returns
        -------
        str
            The path to the YAML file

        """
        return self._yaml_path

    @property
    def data(self) -> DataDict:
        """
        Gets the processed data from the YAML file.

        Returns
        -------
            The processed data.

        """
        return self._data


def yaml_extract(yaml_path: str) -> SystemConfiguration:
    """
    Extract and process YAML data from a given file path.

    Uses the YamlReader context manager to read YAML data from the provided file

    Parameters
    ----------
    yaml_path : str
        The path to the YAML file relative to the system path

    Returns
    -------
    SystemConfiguration
        An object containing the processed YAML data, split into multiple properties

    """
    with YamlReader(
        yaml_path=str(Path(__file__).resolve().parent.parent.parent.parent / yaml_path),
        loader_instance=SuperLoader,
    ) as data:
        _data = data

    return SystemConfiguration(yaml_path=yaml_path, yaml_data=_data)
