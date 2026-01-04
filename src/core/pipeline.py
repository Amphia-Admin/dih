"""Pipeline base classes and decorators."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING

from src.core.result import ProcessingResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.core.types import PipelineMetadata, SparkConfig


class AbstractProcessingComponent(ABC):
    """Base class for all processing components."""

    static_config: SparkConfig = {}

    def __init__(self) -> None:
        self._inputs: dict[str, DataFrame] = {}
        self._metadata: PipelineMetadata = {}
        self._outputs: ProcessingResult = ProcessingResult()

    @abstractmethod
    def process(self) -> None:
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
    def metadata(self) -> PipelineMetadata:
        """Return metadata dictionary."""
        return self._metadata

    @metadata.setter
    def metadata(self, value: PipelineMetadata) -> None:
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

    def process(self) -> None:
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


type PipelineDecorator = Callable[[type[Pipeline]], type[Pipeline]]


def pipeline_definition(name: str, description: str = "") -> PipelineDecorator:
    """
    Define pipeline metadata via decorator.

    Parameters
    ----------
    name : str
        The name of the pipeline.
    description : str, optional
        A description of the pipeline.

    Returns
    -------
    PipelineDecorator
        A decorator that sets the pipeline name and description.
    """

    def decorator(pipeline: type[Pipeline]) -> type[Pipeline]:
        pipeline.name = name
        pipeline.description = description
        return pipeline

    return decorator
