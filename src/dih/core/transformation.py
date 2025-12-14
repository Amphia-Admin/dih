"""Transformation base classes and decorators."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from src.dih.core.result import ProcessingResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class AbstractProcessingComponent(ABC):
    """Base class for all processing components."""

    static_config: dict[str, Any] = {}

    def __init__(self) -> None:
        self._inputs: dict[str, DataFrame] = {}
        self._metadata: dict[str, Any] = {}
        self._outputs: ProcessingResult = ProcessingResult()

    @abstractmethod
    def process(self, *args: Any, **kwargs: Any) -> None:
        """Execute the processing logic."""
        ...

    @property
    def inputs(self) -> dict[str, DataFrame]:
        """Return input DataFrames."""
        return self._inputs

    @inputs.setter
    def inputs(self, value: dict[str, DataFrame]) -> None:
        """Set input DataFrames."""
        self._inputs.update(value)

    @property
    def metadata(self) -> dict[str, Any]:
        """Return metadata dictionary."""
        return self._metadata

    @metadata.setter
    def metadata(self, value: dict[str, Any]) -> None:
        """Set metadata."""
        self._metadata = value

    @property
    def outputs(self) -> ProcessingResult:
        """Return output results collection."""
        return self._outputs


class Transformation(AbstractProcessingComponent):
    """Base class for transformations/pipelines."""

    name: str = ""
    description: str = ""

    def __init__(self) -> None:
        super().__init__()

    def process(self, *args: Any, **kwargs: Any) -> None:
        """Override in subclass to implement transformation logic."""

    def __repr__(self) -> str:
        desc = self.description[:50] + "..." if len(self.description) > 50 else self.description
        return f"<{self.__class__.__name__}: {desc}>"


def transformation_definition(name: str, description: str = "") -> Any:
    """Define transformation metadata via decorator."""

    def decorator(transform: type[Transformation]) -> type[Transformation]:
        if not issubclass(transform, Transformation):
            msg = "Expected subclass of 'Transformation'"
            raise ValueError(msg)

        transform.name = name
        transform.description = description
        return transform

    return decorator
