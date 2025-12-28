"""Writer registration system."""

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from src.core.table_interfaces import TableDefinition
    from src.writers.base_spark_writer import AbstractWriter
    from src.core.pipeline import Pipeline

logger = logging.getLogger(__name__)


class WriterRegistry:
    """Singleton registry for writers."""

    _shared_state: dict[str, Any] | None = None

    class RegisteredWriter:
        """Container for registered writer configuration."""

        def __init__(
            self,
            definition_type: type[TableDefinition],
            writer: type[AbstractWriter],
            **kwargs: Any,
        ) -> None:
            self._definition_type = definition_type
            self.aliases: list[str] = []
            self.transforms: list[type[Pipeline]] = []
            self._writer = writer
            self._kwargs = kwargs

        def __eq__(self, other: object) -> bool:
            return isinstance(other, WriterRegistry.RegisteredWriter) and hash(self) == hash(other)

        def __hash__(self) -> int:
            return hash((self._definition_type, self._writer))

        def write(
            self,
            df: DataFrame,
            catalog: str | None = None,
            volumes: dict[str, str] | None = None,
        ) -> None:
            """Instantiate and execute the writer."""
            def_obj = self._definition_type()
            def_obj.catalog = catalog
            def_obj.volumes = volumes

            wrt_obj = self._writer()
            wrt_obj.write(df, def_obj, **self._kwargs)

    def __init__(self) -> None:
        if not WriterRegistry._shared_state:
            logger.info("Initialising WriterRegistry")
            WriterRegistry._shared_state = self.__dict__
            self._alias_lookup: dict[str, WriterRegistry.RegisteredWriter] = {}
            self._registered_writers: dict[int, WriterRegistry.RegisteredWriter] = {}
        else:
            self.__dict__ = WriterRegistry._shared_state

    def _register(
        self,
        name: str,
        definition_type: type[TableDefinition],
        writer: type[AbstractWriter],
        transformation: type[Pipeline],
        **kwargs: Any,
    ) -> None:
        """Register a writer internally with the registry."""
        writer_identity = hash((definition_type, writer))

        if writer_identity in self._registered_writers:
            registered_writer = self._registered_writers[writer_identity]
        else:
            registered_writer = self.RegisteredWriter(
                definition_type=definition_type,
                writer=writer,
                **kwargs,
            )
            self._registered_writers[writer_identity] = registered_writer

        registered_writer.transforms.append(transformation)
        if name not in registered_writer.aliases:
            registered_writer.aliases.append(name)
            self._alias_lookup[name] = registered_writer
            logger.debug(f"Registered writer '{name}' -> {definition_type.__name__} for {transformation.__name__}")

    def register(
        self,
        alias: str,
        definition_type: type[TableDefinition],
        writer: type[AbstractWriter],
        transformation: type[Pipeline],
        **kwargs: Any,
    ) -> None:
        """Public registration method."""
        self._register(alias, definition_type, writer, transformation, **kwargs)

    @property
    def writers(self) -> list[RegisteredWriter]:
        """Return all registered writers."""
        return list(self._alias_lookup.values())

    def get_writers(self, transformation: type[Pipeline]) -> list[RegisteredWriter]:
        """Get all writers for a specific transformation."""
        writers = [
            writer
            for writer in self._registered_writers.values()
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
