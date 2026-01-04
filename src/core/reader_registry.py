"""Reader registration system."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.core.pipeline import Pipeline
    from src.core.table_interfaces import TableDefinition
    from src.core.types import WriteOptionsValue
    from src.readers.base_spark_reader import AbstractReader

logger = logging.getLogger(__name__)


@dataclass
class RegisteredReader:
    """Container for registered reader configuration."""

    definition_type: type[TableDefinition]
    reader: type[AbstractReader]
    kwargs: dict[str, str | int | bool | list[str] | None] = field(default_factory=dict)
    aliases: list[str] = field(default_factory=list)
    transforms: list[type[Pipeline]] = field(default_factory=list)

    def __eq__(self, other: object) -> bool:
        """Compare two RegisteredReader instances by their hash values."""
        return isinstance(other, RegisteredReader) and hash(self) == hash(other)

    def __hash__(self) -> int:
        """Return hash based on definition type and reader class."""
        return hash((self.definition_type, self.reader))

    def read(
        self,
        catalog: str | None = None,
        volumes: dict[str, str] | None = None,
    ) -> AbstractReader:
        """Instantiate and execute the reader."""
        def_obj = self.definition_type()
        def_obj.catalog = catalog
        def_obj.volumes = volumes

        rdr_obj = self.reader()
        rdr_obj.read(def_obj)

        return rdr_obj


@dataclass
class _ReaderRegistryState:
    """Internal state for ReaderRegistry singleton."""

    alias_lookup: dict[str, RegisteredReader] = field(default_factory=dict)
    registered_readers: dict[int, RegisteredReader] = field(default_factory=dict)


class ReaderRegistry:
    """Singleton registry for readers."""

    _shared_state: _ReaderRegistryState | None = None

    def __init__(self) -> None:
        if ReaderRegistry._shared_state is None:
            logger.info("Initialising ReaderRegistry")
            ReaderRegistry._shared_state = _ReaderRegistryState()
        self._state = ReaderRegistry._shared_state

    def _register(
        self,
        name: str,
        definition_type: type[TableDefinition],
        reader: type[AbstractReader],
        transformation: type[Pipeline],
        **kwargs: WriteOptionsValue,
    ) -> None:
        """Register a reader internally with the registry."""
        reader_identity = hash((definition_type, reader))

        if reader_identity in self._state.registered_readers:
            registered_reader = self._state.registered_readers[reader_identity]
        else:
            registered_reader = RegisteredReader(
                definition_type=definition_type,
                reader=reader,
                kwargs=dict(kwargs),
            )
            self._state.registered_readers[reader_identity] = registered_reader

        registered_reader.transforms.append(transformation)
        if name not in registered_reader.aliases:
            registered_reader.aliases.append(name)
            self._state.alias_lookup[name] = registered_reader
            logger.debug(
                f"Registered reader '{name}' -> {definition_type.__name__} "
                f"for {transformation.__name__}"
            )

    def register(
        self,
        alias: str,
        definition_type: type[TableDefinition],
        reader: type[AbstractReader],
        transformation: type[Pipeline],
        **kwargs: WriteOptionsValue,
    ) -> None:
        """Public registration method."""
        self._register(alias, definition_type, reader, transformation, **kwargs)

    @property
    def readers(self) -> list[RegisteredReader]:
        """Return all registered readers."""
        return list(self._state.alias_lookup.values())

    def get_readers(self, pipeline: type[Pipeline]) -> list[RegisteredReader]:
        """Get all readers for a specific pipeline."""
        readers = [
            reader
            for reader in self._state.registered_readers.values()
            if pipeline in reader.transforms
        ]
        if readers:
            aliases = [r.aliases[0] for r in readers]
            logger.debug(f"Found {len(readers)} reader(s) for {pipeline.__name__}: {aliases}")
        else:
            logger.warning(f"No readers registered for pipeline: {pipeline.__name__}")
        return readers


class register_reader:
    """Decorator to register a reader with a transformation."""

    def __init__(
        self,
        definition: type[TableDefinition],
        reader: type[AbstractReader],
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

        self._reader = reader
        self._kwargs = kwargs
        self._registry = ReaderRegistry()

    def __call__(self, transformation: type[Pipeline]) -> type[Pipeline]:
        """Register the transformation with the reader."""
        self._registry.register(
            alias=self._alias,
            definition_type=self._definition,
            transformation=transformation,
            reader=self._reader,
            **self._kwargs,
        )
        return transformation
