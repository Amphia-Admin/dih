"""Reader registration system."""

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.dih.core.table_interfaces import TableDefinition
    from src.dih.readers.base_spark_reader import AbstractReader
    from src.dih.core.pipeline import Pipeline

logger = logging.getLogger(__name__)


class ReaderRegistry:
    """Singleton registry for readers."""

    _shared_state: dict[str, Any] | None = None

    class RegisteredReader:
        """Container for registered reader configuration."""

        def __init__(
            self,
            definition_type: type[TableDefinition],
            reader: type[AbstractReader],
            **kwargs: Any,
        ) -> None:
            self._definition_type = definition_type
            self.aliases: list[str] = []
            self.transforms: list[type[Pipeline]] = []
            self._reader = reader
            self._kwargs = kwargs

        def __eq__(self, other: object) -> bool:
            return isinstance(other, ReaderRegistry.RegisteredReader) and hash(self) == hash(other)

        def __hash__(self) -> int:
            return hash((self._definition_type, self._reader))

        def read(self, root_path: str) -> AbstractReader:
            """Instantiate and execute the reader."""
            def_obj = self._definition_type()
            def_obj.root_path = root_path

            rdr_obj = self._reader()
            rdr_obj.read(def_obj)

            return rdr_obj

    def __init__(self) -> None:
        if not ReaderRegistry._shared_state:
            logger.info("Initialising ReaderRegistry")
            ReaderRegistry._shared_state = self.__dict__
            self._alias_lookup: dict[str, ReaderRegistry.RegisteredReader] = {}
            self._registered_readers: dict[int, ReaderRegistry.RegisteredReader] = {}
        else:
            self.__dict__ = ReaderRegistry._shared_state

    def _register(
        self,
        name: str,
        definition_type: type[TableDefinition],
        reader: type[AbstractReader],
        transformation: type[Pipeline],
        **kwargs: Any,
    ) -> None:
        """Register a reader internally with the registry."""
        reader_identity = hash((definition_type, reader))

        if reader_identity in self._registered_readers:
            registered_reader = self._registered_readers[reader_identity]
        else:
            registered_reader = self.RegisteredReader(
                definition_type=definition_type,
                reader=reader,
                **kwargs,
            )
            self._registered_readers[reader_identity] = registered_reader

        registered_reader.transforms.append(transformation)
        if name not in registered_reader.aliases:
            registered_reader.aliases.append(name)
            self._alias_lookup[name] = registered_reader

    def register(
        self,
        alias: str,
        definition_type: type[TableDefinition],
        reader: type[AbstractReader],
        transformation: type[Pipeline],
        **kwargs: Any,
    ) -> None:
        """Public registration method."""
        self._register(alias, definition_type, reader, transformation, **kwargs)

    @property
    def readers(self) -> list[RegisteredReader]:
        """Return all registered readers."""
        return list(self._alias_lookup.values())

    def get_readers(self, pipeline: type[Pipeline]) -> list[RegisteredReader]:
        """Get all readers for a specific pipeline."""
        return [
            reader
            for reader in self._registered_readers.values()
            if pipeline in reader.transforms
        ]


class register_reader:
    """Decorator to register a reader with a transformation."""

    def __init__(
        self,
        definition: type[TableDefinition],
        reader: type[AbstractReader],
        alias: str | None = None,
        **kwargs: Any,
    ) -> None:
        self._definition = definition

        if alias is not None:
            self._alias = alias
        elif hasattr(definition, "default_alias"):
            self._alias = definition.default_alias  # type: ignore[attr-defined]
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
