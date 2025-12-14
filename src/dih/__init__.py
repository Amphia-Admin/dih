"""DIH - Data Ingestion Hub Framework."""

from src.dih.constants import FileFormat, LakeLayer, WriteMode
from src.dih.core.config import create_run_config
from src.dih.core.interfaces import (
    IReader,
    IWriter,
    TableDefinition,
    TargetTableDefMixin,
)
from src.dih.core.reader_registry import ReaderRegistry, register_reader
from src.dih.core.result import ProcessingResult
from src.dih.core.runner import Runner
from src.dih.core.transformation import Transformation, transformation_definition
from src.dih.core.writer_registry import WriterRegistry, register_writer
from src.dih.delta import (
    DeltaMergeAutoPartitionWriter,
    DeltaMergeWriter,
    DeltaWriterBase,
)
from src.dih.io.spark_reader import SparkDataFrameReader
from src.dih.io.spark_writer import SparkDataFrameWriter
from src.dih.utils.loader import DynamicLoader
from src.dih.utils.paths import PathBuilder, path_exists

__version__ = "0.1.0"
__all__ = [
    "DeltaMergeAutoPartitionWriter",
    "DeltaMergeWriter",
    # Delta Writers
    "DeltaWriterBase",
    "DynamicLoader",
    # Constants
    "FileFormat",
    # Core interfaces
    "IReader",
    "IWriter",
    "LakeLayer",
    # Utilities
    "PathBuilder",
    "ProcessingResult",
    # Registries
    "ReaderRegistry",
    # Runner
    "Runner",
    # I/O
    "SparkDataFrameReader",
    "SparkDataFrameWriter",
    "TableDefinition",
    "TargetTableDefMixin",
    # Transformation
    "Transformation",
    "WriteMode",
    "WriterRegistry",
    # Configuration
    "create_run_config",
    "path_exists",
    "register_reader",
    "register_writer",
    "transformation_definition",
]
