"""Dynamic class loading utilities."""

import importlib
import logging
from typing import Any

from src.dih.core.transformation import Transformation

logger = logging.getLogger(__name__)


class DynamicLoader:
    """Utilities for dynamic class loading from string references."""

    @staticmethod
    def load_class(fqn: str) -> type[Any]:
        """
        Load a class from a fully qualified name.

        Parameters
        ----------
        fqn : str
            Fully qualified name (e.g., 'package.module.ClassName')

        Returns
        -------
        type[Any]
            The loaded class (not an instance)

        Raises
        ------
        ImportError
            If the module cannot be imported
        AttributeError
            If the class cannot be found in the module
        """
        try:
            # Split module path and class name
            module_path, class_name = fqn.rsplit(".", 1)
        except ValueError as e:
            msg = (
                f"Invalid fully qualified name: '{fqn}'. "
                f"Expected format: 'package.module.ClassName'"
            )
            raise ValueError(msg) from e

        try:
            # Import the module
            module = importlib.import_module(module_path)
            logger.debug(f"Imported module: {module_path}")
        except ImportError as e:
            msg = f"Failed to import module '{module_path}' from '{fqn}'"
            logger.error(msg)
            raise ImportError(msg) from e

        try:
            # Get the class from the module
            cls: type[Any] = getattr(module, class_name)
            logger.debug(f"Loaded class: {class_name}")
            return cls
        except AttributeError as e:
            msg = (
                f"Class '{class_name}' not found in module '{module_path}'. "
                f"Available names: {dir(module)}"
            )
            logger.error(msg)
            raise AttributeError(msg) from e

    @staticmethod
    def load_transformation(
        reference: str | type[Transformation],
    ) -> type[Transformation]:
        """
        Load transformation class from string FQN or return class if already loaded.

        Parameters
        ----------
        reference : str | type[Transformation]
            Either a fully qualified name string or a Transformation class

        Returns
        -------
        type[Transformation]
            Transformation class (not instance)

        Raises
        ------
        ImportError
            If module cannot be imported
        AttributeError
            If class cannot be found in module
        TypeError
            If loaded class is not a Transformation subclass
        """
        # If already a class, validate and return
        if not isinstance(reference, str):
            if not isinstance(reference, type) or not issubclass(reference, Transformation):
                msg = (
                    f"Expected Transformation class or string, got {type(reference)}. "
                    f"Ensure the class inherits from Transformation."
                )
                raise TypeError(msg)
            logger.debug(f"Using provided class: {reference.__name__}")
            return reference

        # Load from string
        logger.info(f"Loading transformation from string: {reference}")
        cls = DynamicLoader.load_class(reference)

        # Validate it's a Transformation subclass
        if not isinstance(cls, type) or not issubclass(cls, Transformation):
            msg = (
                f"Loaded class '{cls}' is not a Transformation subclass. "
                f"Ensure the class inherits from Transformation."
            )
            raise TypeError(msg)

        logger.info(f"Successfully loaded transformation: {cls.__name__}")
        return cls
