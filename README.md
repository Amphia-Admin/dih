# DIH - Data Ingestion Hub

Type-safe, decorator-based ETL framework for Databricks Delta Lake.

## Status

**Phase 1 Complete** ✓ - Foundation established

### Completed Features

- ✅ Type-safe core interfaces (TableDefinition, IReader, IWriter)
- ✅ Decorator-based registry pattern (@register_reader, @register_writer)
- ✅ Transformation base classes with inputs/outputs pattern
- ✅ ProcessingResult collection (Mapping interface)
- ✅ Generic Spark DataFrame reader/writer
- ✅ Constants and enums (FileFormat, WriteMode, LakeLayer)
- ✅ Comprehensive test suite with Spark/Delta fixtures
- ✅ Development infrastructure (mypy, ruff, pytest, tox, pre-commit, CI/CD)

### Installation

```bash
# Install in development mode
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install
```

### Quick Start

```python
from dih import (
    TableDefinition,
    TargetTableDefMixin,
    SparkDataFrameReader,
    SparkDataFrameWriter,
    Transformation,
    transformation_definition,
    register_reader,
    register_writer,
)

# Define source table
class SourceTable(TableDefinition):
    @property
    def path(self) -> str:
        return f"{self.root_path}/raw/data.csv"

    @property
    def format(self) -> str:
        return "csv"

    @property
    def options(self) -> dict:
        return {"header": "true", "inferSchema": "true"}

# Define target table
class TargetTable(TableDefinition, TargetTableDefMixin):
    @property
    def path(self) -> str:
        return f"{self.root_path}/bronze/data"

    @property
    def format(self) -> str:
        return "delta"

    @property
    def primary_keys(self) -> list[str]:
        return ["id"]

# Create transformation with decorators
@transformation_definition(name="my_etl", description="ETL pipeline")
@register_reader(SourceTable, SparkDataFrameReader, alias="input")
@register_writer(TargetTable, SparkDataFrameWriter, alias="output")
class MyTransformation(Transformation):
    def process(self) -> None:
        df = self.inputs["input"]

        # Your transformation logic here
        transformed = df.filter("status = 'active'")

        self.outputs.add("output", transformed)
```

## Development

### Run Tests

```bash
# All tests
pytest

# With coverage
pytest --cov=dih --cov-report=html

# Specific test file
pytest tests/test_core/test_interfaces.py
```

### Type Checking

```bash
mypy src/dih
```

### Linting

```bash
# Check
ruff check src/dih tests

# Format
ruff format src/dih tests
```

### Multi-Environment Testing

```bash
# Test across Python versions
tox

# Specific environment
tox -e py310
tox -e mypy
tox -e ruff
```

## Roadmap

- **Phase 2** (In Progress): Runner orchestrator, path utilities
- **Phase 3** (Planned): Delta merge strategies (business keys, partition lock, replace-where)
- **Phase 4** (Planned): Utilities (hashing, schema), transformations, examples

## Architecture

```
src/dih/
├── core/              # Abstract interfaces and registries
│   ├── interfaces.py  # TableDefinition, IReader, IWriter
│   ├── transformation.py
│   ├── result.py      # ProcessingResult
│   ├── reader_registry.py
│   └── writer_registry.py
├── io/                # Reader/writer implementations
│   ├── spark_reader.py
│   └── spark_writer.py
├── constants.py       # Enums (FileFormat, WriteMode, LakeLayer)
└── __init__.py        # Public API
```

## License

MIT
