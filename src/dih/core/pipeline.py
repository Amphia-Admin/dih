"""Pipeline base classes and decorators."""

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


class Pipeline(AbstractProcessingComponent):
    """Base class for pipelines."""

    name: str = ""
    description: str = ""

    def __init__(self) -> None:
        super().__init__()

    def process(self, *args: Any, **kwargs: Any) -> None:
        """Override in subclass to implement pipeline logic."""

    def __repr__(self) -> str:
        """
        Return string representation of the pipeline.

        Returns
        -------
        str
            String representation.
        """
        desc = self.description[:50] + "..." if len(self.description) > 50 else self.description
        return f"<{self.__class__.__name__}: {desc}>"


def pipeline_definition(name: str, description: str = "") -> Any:
    """Define pipeline metadata via decorator."""

    def decorator(pipeline: type[Pipeline]) -> type[Pipeline]:
        if not issubclass(pipeline, Pipeline):
            msg = "Expected subclass of 'Pipeline'"
            raise ValueError(msg)

        pipeline.name = name
        pipeline.description = description
        return pipeline

    return decorator
