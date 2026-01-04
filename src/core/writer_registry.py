"""Writer registration system."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.core.pipeline import Pipeline
    from src.core.table_interfaces import TableDefinition
    from src.core.types import WriteOptionsValue
    from src.writers.base_spark_writer import AbstractWriter

logger = logging.getLogger(__name__)


@dataclass
class RegisteredWriter:
    """Container for registered writer configuration."""

    definition_type: type[TableDefinition]
    writer: type[AbstractWriter]
    kwargs: dict[str, str | int | bool | list[str] | None] = field(default_factory=dict)
    aliases: list[str] = field(default_factory=list)
    transforms: list[type[Pipeline]] = field(default_factory=list)

    def __eq__(self, other: object) -> bool:
        """Compare two RegisteredWriter instances by their hash values."""
        return isinstance(other, RegisteredWriter) and hash(self) == hash(other)

    def __hash__(self) -> int:
        """Return hash based on definition type and writer class."""
        return hash((self.definition_type, self.writer))

    def write(
        self,
        df: DataFrame,
        catalog: str | None = None,
        volumes: dict[str, str] | None = None,
    ) -> None:
        """Instantiate and execute the writer."""
        def_obj = self.definition_type()
        def_obj.catalog = catalog
        def_obj.volumes = volumes

        wrt_obj = self.writer()
        wrt_obj.write(df, def_obj, **self.kwargs)


@dataclass
class _WriterRegistryState:
    """Internal state for WriterRegistry singleton."""

    alias_lookup: dict[str, RegisteredWriter] = field(default_factory=dict)
    registered_writers: dict[int, RegisteredWriter] = field(default_factory=dict)


class WriterRegistry:
    """Singleton registry for writers."""

    _shared_state: _WriterRegistryState | None = None

    def __init__(self) -> None:
        if WriterRegistry._shared_state is None:
            logger.info("Initialising WriterRegistry")
            WriterRegistry._shared_state = _WriterRegistryState()
        self._state = WriterRegistry._shared_state

    def _register(
        self,
        name: str,
        definition_type: type[TableDefinition],
        writer: type[AbstractWriter],
        transformation: type[Pipeline],
        **kwargs: WriteOptionsValue,
    ) -> None:
        """Register a writer internally with the registry."""
        writer_identity = hash((definition_type, writer))

        if writer_identity in self._state.registered_writers:
            registered_writer = self._state.registered_writers[writer_identity]
        else:
            registered_writer = RegisteredWriter(
                definition_type=definition_type,
                writer=writer,
                kwargs=dict(kwargs),
            )
            self._state.registered_writers[writer_identity] = registered_writer

        registered_writer.transforms.append(transformation)
        if name not in registered_writer.aliases:
            registered_writer.aliases.append(name)
            self._state.alias_lookup[name] = registered_writer
            logger.debug(
                f"Registered writer '{name}' -> {definition_type.__name__} "
                f"for {transformation.__name__}"
            )

    def register(
        self,
        alias: str,
        definition_type: type[TableDefinition],
        writer: type[AbstractWriter],
        transformation: type[Pipeline],
        **kwargs: WriteOptionsValue,
    ) -> None:
        """Public registration method."""
        self._register(alias, definition_type, writer, transformation, **kwargs)

    @property
    def writers(self) -> list[RegisteredWriter]:
        """Return all registered writers."""
        return list(self._state.alias_lookup.values())

    def get_writers(self, transformation: type[Pipeline]) -> list[RegisteredWriter]:
        """Get all writers for a specific transformation."""
        writers = [
            writer
            for writer in self._state.registered_writers.values()
            if transformation in writer.transforms
        ]
        if writers:
            aliases = [w.aliases[0] for w in writers]
            logger.debug(f"Found {len(writers)} writer(s) for {transformation.__name__}: {aliases}")
        else:
            logger.warning(f"No writers registered for pipeline: {transformation.__name__}")
        return writers


class register_writer:
    """Decorator to register a writer with a transformation."""

    def __init__(
        self,
        definition: type[TableDefinition],
        writer: type[AbstractWriter],
        alias: str | None = None,
        **kwargs: WriteOptionsValue,
    ) -> None:
        self._definition = definition

        if alias is not None:
            self._alias = alias
        elif hasattr(definition, "default_alias"):
            self._alias = definition.default_alias  # type: ignore[assignment]
        else:
            msg = (
                f"No alias defined for '{definition}'. "
                f"Provide an alias through decorator or define a default_alias in the definition"
            )
            raise ValueError(msg)

        self._writer = writer
        self._kwargs = kwargs
        self._registry = WriterRegistry()

    def __call__(self, transformation: type[Pipeline]) -> type[Pipeline]:
        """Register the transformation with the writer."""
        self._registry.register(
            alias=self._alias,
            definition_type=self._definition,
            transformation=transformation,
            writer=self._writer,
            **self._kwargs,
        )
        return transformation
