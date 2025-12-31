# IH Ingestion Framework - Core Components

This directory contains the core framework for building medallion architecture data pipelines. The framework provides a declarative, decorator-based approach to defining ETL pipelines with first-class Delta Lake support.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Pipeline Execution                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   Reader    │───>│   Pipeline  │───>│   Writer    │───>│   Target    │  │
│  │  Registry   │    │   process() │    │  Registry   │    │   Table     │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│        │                  │                  │                             │
│        │            ┌─────┴─────┐            │                             │
│        │            │  Runner   │            │                             │
│        └────────────│ (orchest.)├────────────┘                             │
│                     └───────────┘                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Directory Structure

```
src/
├── core/                       # Core framework components
│   ├── pipeline.py             # Pipeline base class and decorators
│   ├── runner.py               # Pipeline orchestrator
│   ├── table_interfaces.py     # Table definition interfaces
│   ├── reader_registry.py      # Reader registration system
│   ├── writer_registry.py      # Writer registration system
│   └── dynamic_loader.py       # Dynamic class loading
│
├── readers/                    # Data readers
│   ├── abstract_reader.py      # Reader interface
│   ├── base_spark_reader.py    # Generic Spark reader
│   ├── delta_table_reader.py   # Delta table reader
│   ├── catalog_table_reader.py # Unity Catalog reader
│   └── auto_loader_reader.py   # Databricks Auto Loader
│
├── writers/                    # Data writers
│   ├── abstract_writer.py      # Writer interface
│   ├── base_spark_writer.py    # Generic Spark writer
│   ├── delta_writer_base.py    # Delta writer base class
│   ├── delta_merge_writer.py   # Delta merge with business keys
│   ├── delta_merge_auto_partition.py  # Partition-aware merge
│   └── utils.py                # Writer utilities
│
├── transforms/                 # Data transformations
│   └── apply_schema.py         # Schema enforcement transform
│
└── constants.py                # Enums and constants
```

---

## Core Components

### 1. Pipeline (`core/pipeline.py`)

The base class for all data pipelines.

**AbstractProcessingComponent:**
```python
class AbstractProcessingComponent(ABC):
    inputs: dict[str, DataFrame]      # Input DataFrames keyed by alias
    outputs: ProcessingResult         # Output DataFrame container
    metadata: dict[str, Any]          # Runtime metadata
    static_config: dict[str, Any]     # Static configuration

    @abstractmethod
    def process(self) -> None:
        """Transform inputs to outputs."""
        pass
```

**Pipeline Class:**
```python
class Pipeline(AbstractProcessingComponent):
    name: str          # Pipeline identifier
    description: str   # Human-readable description
```

**@pipeline_definition Decorator:**
```python
from src.core.pipeline import Pipeline, pipeline_definition

@pipeline_definition(name="BronzeOrdersPipeline", description="Ingest orders to bronze")
class BronzeOrdersPipeline(Pipeline):
    def process(self) -> None:
        df = self.inputs["landed_orders"]
        # Transform...
        self.outputs.add("bronze_orders", result)
```

### 2. Runner (`core/runner.py`)

Orchestrates the complete ETL lifecycle.

**Execution Steps:**
1. **Apply Spark config**: Runtime Spark configuration
2. **Initialise pipeline**: Load class, create instance
3. **Extract inputs**: Read data via registered readers
4. **Inject metadata**: Pass metadata to pipeline
5. **Execute pipeline**: Call `process()` method
6. **Load outputs**: Write data via registered writers

**Usage:**
```python
from src.core.runner import Runner

runner = Runner(config=pipeline_config, pipeline=MyPipeline)
runner.run()
```

**Features:**
- Timing instrumentation at each stage
- DataFrame statistics logging (row/column counts)
- Exception handling with duration reporting
- Dynamic pipeline loading by string name

### 3. Table Interfaces (`core/table_interfaces.py`)

**TableDefinition (Abstract):**
```python
class TableDefinition(ABC):
    catalog: str              # Injected catalog name
    volumes: dict[str, str]   # Injected volume mappings

    def get_volume(self, name: str) -> str:
        """Get volume path by name."""
        return self.volumes[name]

    @property
    @abstractmethod
    def path(self) -> str | None:
        """File system path (None for catalog tables)."""
        pass

    @property
    @abstractmethod
    def format(self) -> str:
        """File format: delta, parquet, csv, json, avro, orc."""
        pass

    # Optional properties
    @property
    def table_name(self) -> str | None:
        """Fully qualified catalog table name."""
        return None

    @property
    def options(self) -> dict[str, Any] | None:
        """Read/write options."""
        return None

    @property
    def schema(self) -> StructType | None:
        """PySpark schema."""
        return None

    @property
    def default_alias(self) -> str | None:
        """Default alias for registry."""
        return None
```

**TargetTableDefMixin (for write operations):**
```python
class TargetTableDefMixin(ABC):
    @property
    @abstractmethod
    def managed(self) -> bool:
        """True = catalog-managed, False = path-based."""
        pass

    @property
    @abstractmethod
    def write_mode(self) -> str:
        """append, overwrite, error, ignore, merge."""
        pass

    @property
    def partition_by(self) -> list[str] | None:
        """Partition columns."""
        return None

    @property
    def primary_keys(self) -> list[str] | None:
        """Business keys for merge operations."""
        return None

    @property
    def merge_options(self) -> dict[str, Any] | None:
        """Delta merge configuration."""
        return None
```

### 4. Reader Registry (`core/reader_registry.py`)

Singleton registry that maps readers to table definitions.

**@register_reader Decorator:**
```python
from src.core.reader_registry import register_reader
from src.readers.base_spark_reader import SparkDataFrameReader

@register_reader(
    definition=LandedOrdersDef,
    reader=SparkDataFrameReader,
    alias="landed_orders"
)
class MyPipeline(Pipeline):
    def process(self) -> None:
        df = self.inputs["landed_orders"]  # Available via alias
```

**Multiple Readers:**
```python
@register_reader(definition=OrdersDef, reader=SparkDataFrameReader, alias="orders")
@register_reader(definition=CustomersDef, reader=DeltaTableReader, alias="customers")
@register_reader(definition=ProductsDef, reader=CatalogTableReader, alias="products")
class EnrichmentPipeline(Pipeline):
    def process(self) -> None:
        orders = self.inputs["orders"]
        customers = self.inputs["customers"]
        products = self.inputs["products"]
```

### 5. Writer Registry (`core/writer_registry.py`)

Singleton registry that maps writers to table definitions.

**@register_writer Decorator:**
```python
from src.core.writer_registry import register_writer
from src.writers.base_spark_writer import SparkDataFrameWriter

@register_writer(
    definition=BronzeOrdersDef,
    writer=SparkDataFrameWriter,
    alias="bronze_orders"
)
class MyPipeline(Pipeline):
    def process(self) -> None:
        # ...
        self.outputs.add("bronze_orders", result_df)  # Writes via alias
```

**Multiple Outputs:**
```python
@register_writer(definition=ValidOrdersDef, writer=DeltaMergeWriter, alias="valid_orders")
@register_writer(definition=InvalidOrdersDef, writer=SparkDataFrameWriter, alias="invalid_orders")
class ValidationPipeline(Pipeline):
    def process(self) -> None:
        valid, invalid = validate(self.inputs["orders"])
        self.outputs.add("valid_orders", valid)
        self.outputs.add("invalid_orders", invalid)
```

---

## Readers

### SparkDataFrameReader

Generic reader for file-based sources (CSV, Parquet, JSON, Delta, Avro, ORC).

```python
from src.readers.base_spark_reader import SparkDataFrameReader

class LandedOrdersDef(TableDefinition):
    @property
    def path(self) -> str:
        return f"{self.get_volume('lake')}/raw/orders/orders.csv"

    @property
    def format(self) -> str:
        return "csv"

    @property
    def options(self) -> dict[str, Any]:
        return {"header": "true", "inferSchema": "true"}

@register_reader(definition=LandedOrdersDef, reader=SparkDataFrameReader, alias="orders")
```

### DeltaTableReader

Specialised reader for Delta tables (path-based or catalog-managed).

```python
from src.readers.delta_table_reader import DeltaTableReader

class BronzeOrdersDef(TableDefinition):
    @property
    def table_name(self) -> str:
        return "bronze.orders"  # Uses spark.table()

    @property
    def path(self) -> str | None:
        return None  # Not needed for managed tables

    @property
    def format(self) -> str:
        return "delta"

@register_reader(definition=BronzeOrdersDef, reader=DeltaTableReader, alias="bronze_orders")
```

### CatalogTableReader

Simple reader for Unity Catalog tables.

```python
from src.readers.catalog_table_reader import CatalogTableReader

@register_reader(definition=CatalogTableDef, reader=CatalogTableReader, alias="catalog_table")
```

### AutoLoaderReader

Databricks Auto Loader for incremental file ingestion.

```python
from src.readers.auto_loader_reader import AutoLoaderReader

class StreamingOrdersDef(TableDefinition):
    @property
    def path(self) -> str:
        return "/Volumes/lake/landing/orders/"

    @property
    def format(self) -> str:
        return "cloudFiles"

    @property
    def options(self) -> dict[str, Any]:
        return {
            "cloudFiles.format": "json",
            "cloudFiles.schemaLocation": "/checkpoints/orders_schema",
        }

@register_reader(definition=StreamingOrdersDef, reader=AutoLoaderReader, alias="orders_stream")
```

---

## Writers

### SparkDataFrameWriter

Generic writer for all Spark-supported formats.

```python
from src.writers.base_spark_writer import SparkDataFrameWriter

class BronzeOrdersDef(TableDefinition, TargetTableDefMixin):
    @property
    def managed(self) -> bool:
        return True

    @property
    def table_name(self) -> str:
        return "bronze.orders"

    @property
    def format(self) -> str:
        return "delta"

    @property
    def write_mode(self) -> str:
        return "overwrite"

    @property
    def partition_by(self) -> list[str]:
        return ["order_date"]

@register_writer(definition=BronzeOrdersDef, writer=SparkDataFrameWriter, alias="bronze_orders")
```

### DeltaMergeWriter

Merge (upsert) writer using business keys.

```python
from src.writers.delta_merge_writer import DeltaMergeWriter

class SilverOrdersDef(TableDefinition, TargetTableDefMixin):
    @property
    def managed(self) -> bool:
        return True

    @property
    def table_name(self) -> str:
        return "silver.orders"

    @property
    def format(self) -> str:
        return "delta"

    @property
    def write_mode(self) -> str:
        return "merge"

    @property
    def primary_keys(self) -> list[str]:
        return ["order_id"]  # Business key for merge condition

    @property
    def merge_options(self) -> dict[str, Any]:
        return {
            "when_matched_update_condition": "src.updated_at > tgt.updated_at",
            "when_matched_delete_condition": "src.is_deleted = true",
            "when_not_matched_insert_condition": "src.is_deleted = false",
            "source_alias": "src",
            "target_alias": "tgt",
        }

@register_writer(definition=SilverOrdersDef, writer=DeltaMergeWriter, alias="silver_orders")
```

**Merge Options:**
| Option | Description |
|--------|-------------|
| `when_matched_update_condition` | SQL condition for updates |
| `when_matched_delete_condition` | SQL condition for deletes |
| `when_not_matched_insert_condition` | SQL condition for inserts |
| `columns_to_update` | Specific columns to update (default: all non-key) |
| `columns_to_insert` | Specific columns to insert (default: all) |
| `source_alias` | Alias for source table (default: "src") |
| `target_alias` | Alias for target table (default: "tgt") |

### DeltaMergeAutoPartitionWriter

Optimised merge that locks only affected partitions.

```python
from src.writers.delta_merge_auto_partition import DeltaMergeAutoPartitionWriter

class PartitionedOrdersDef(TableDefinition, TargetTableDefMixin):
    @property
    def managed(self) -> bool:
        return True

    @property
    def table_name(self) -> str:
        return "silver.orders"

    @property
    def format(self) -> str:
        return "delta"

    @property
    def write_mode(self) -> str:
        return "merge"

    @property
    def primary_keys(self) -> list[str]:
        return ["order_id"]

    @property
    def partition_by(self) -> list[str]:
        return ["order_date"]  # Required for auto-partition merge

@register_writer(definition=PartitionedOrdersDef, writer=DeltaMergeAutoPartitionWriter, alias="orders")
```

**How it works:**
1. Extracts distinct partition values from source DataFrame
2. Builds partition predicate (`order_date IN ('2025-01-01', '2025-01-02')`)
3. Merge only locks/scans affected partitions

---

## Transforms

### apply_schema (`transforms/apply_schema.py`)

Enforces a target schema on a DataFrame.

**Features:**
- Adds missing columns as NULL with correct type
- Casts mismatched types to target type
- Drops extra columns not in schema
- Reorders columns to match schema order
- Short-circuits if schemas already match

```python
from src.transforms.apply_schema import apply_schema
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

target_schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
])

df_cleaned = apply_schema(raw_df, target_schema)
```

---

## Constants (`constants.py`)

**LakeLayer Enum:**
```python
class LakeLayer(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    LANDING = "landing"
```

**FileFormat Enum:**
```python
class FileFormat(Enum):
    DELTA = "delta"
    PARQUET = "parquet"
    CSV = "csv"
    JSON = "json"
    AVRO = "avro"
    ORC = "orc"
```

---

## Onboarding a New Pipeline

Follow these steps to create a new pipeline in the medallion architecture.

### Step 1: Create Source Table Definition

```python
# pipelines/landed/definitions/landed_orders_def.py
from pathlib import Path
from typing import Any
from src.core.table_interfaces import TableDefinition
from src.constants import FileFormat


class LandedOrdersDef(TableDefinition):
    """Raw orders CSV data."""

    @property
    def path(self) -> str:
        lake_path = self.get_volume("lake")
        return str(Path(lake_path) / "raw/orders/orders.csv")

    @property
    def format(self) -> str:
        return FileFormat.CSV.value

    @property
    def options(self) -> dict[str, Any]:
        return {"header": "true", "inferSchema": "true"}

    @property
    def default_alias(self) -> str:
        return "landed_orders"
```

### Step 2: Create Target Table Definition

```python
# pipelines/bronze/definitions/bronze_orders_def.py
from src.core.table_interfaces import TableDefinition, TargetTableDefMixin
from src.constants import FileFormat, LakeLayer


class BronzeOrdersDef(TableDefinition, TargetTableDefMixin):
    """Bronze layer orders table."""

    @property
    def managed(self) -> bool:
        return True

    @property
    def table_name(self) -> str:
        return f"{LakeLayer.BRONZE.value}.orders"

    @property
    def path(self) -> str | None:
        return None

    @property
    def format(self) -> str:
        return FileFormat.DELTA.value

    @property
    def write_mode(self) -> str:
        return "overwrite"

    @property
    def default_alias(self) -> str:
        return "bronze_orders"
```

### Step 3: Define Schema (Recommended)

```python
# pipelines/bronze/schemas/bronze_orders_schema.py
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("order_date", StringType(), nullable=False),
    StructField("total_amount", DecimalType(10, 2), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("ingestion_timestamp", TimestampType(), nullable=False),
    StructField("source_file", StringType(), nullable=False),
])
```

### Step 4: Create Pipeline

```python
# pipelines/bronze/pipelines/bronze_orders_pipeline.py
from pyspark.sql import functions as F

from src.core.pipeline import Pipeline, pipeline_definition
from src.core.reader_registry import register_reader
from src.core.writer_registry import register_writer
from src.readers.base_spark_reader import SparkDataFrameReader
from src.writers.base_spark_writer import SparkDataFrameWriter
from src.transforms.apply_schema import apply_schema

from pipelines.landed.definitions.landed_orders_def import LandedOrdersDef
from pipelines.bronze.definitions.bronze_orders_def import BronzeOrdersDef
from pipelines.bronze.schemas.bronze_orders_schema import BRONZE_ORDERS_SCHEMA


@pipeline_definition(name="BronzeOrdersPipeline", description="Ingest orders to bronze layer")
@register_reader(definition=LandedOrdersDef, reader=SparkDataFrameReader, alias="landed_orders")
@register_writer(definition=BronzeOrdersDef, writer=SparkDataFrameWriter, alias="bronze_orders")
class BronzeOrdersPipeline(Pipeline):
    """Bronze layer pipeline for orders data."""

    def process(self) -> None:
        # 1. Get input DataFrame
        df = self.inputs["landed_orders"]

        # 2. Add metadata columns
        df_with_metadata = (
            df
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("source_file", F.input_file_name())
        )

        # 3. Apply target schema
        df_final = apply_schema(df_with_metadata, BRONZE_ORDERS_SCHEMA)

        # 4. Register output
        self.outputs.add("bronze_orders", df_final)
```

### Step 5: Create Runner Script

```python
# run_bronze_orders.py
from loadcore.environment import Environment
from src.core.runner import Runner
from pipelines.bronze.pipelines.bronze_orders_pipeline import BronzeOrdersPipeline

# Initialise environment
env = Environment("./env.config.yaml")

# Create pipeline configuration
config = env.for_pipeline(
    metadata={
        "name": "bronze_orders",
        "version": "1.0",
    },
    static_config={
        "enable_dedup": True,
    },
)

# Run pipeline
runner = Runner(config=config, pipeline=BronzeOrdersPipeline)
runner.run()
```

### Step 6: Update `__init__.py` Files

Export your new components:

```python
# pipelines/landed/definitions/__init__.py
from pipelines.landed.definitions.landed_orders_def import LandedOrdersDef

# pipelines/bronze/definitions/__init__.py
from pipelines.bronze.definitions.bronze_orders_def import BronzeOrdersDef

# pipelines/bronze/schemas/__init__.py
from pipelines.bronze.schemas.bronze_orders_schema import BRONZE_ORDERS_SCHEMA

# pipelines/bronze/pipelines/__init__.py
from pipelines.bronze.pipelines.bronze_orders_pipeline import BronzeOrdersPipeline
```

---

## Medallion Architecture Layers

| Layer | Purpose | Typical Operations |
|-------|---------|-------------------|
| **Landed** | Raw source data | Minimal transformation, preserve original |
| **Bronze** | Cleansed data | Type casting, metadata columns, de-duplication |
| **Silver** | Conformed data | Business logic, joins, aggregations |
| **Gold** | Business-ready | Final aggregations, denormalisation |

### Layer Progression Example

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│ Landed  │────>│ Bronze  │────>│ Silver  │────>│  Gold   │
│  (CSV)  │     │ (Delta) │     │ (Delta) │     │ (Delta) │
└─────────┘     └─────────┘     └─────────┘     └─────────┘
   Raw            Cleansed       Conformed       Business
   Files          Tables         Tables          Tables
```

---

## Best Practices

1. **Use schemas**: Always define explicit schemas for type safety
2. **Use volumes**: Never hardcode paths; use `get_volume()` for portability
3. **Use managed tables**: Prefer catalog-managed Delta tables
4. **Add metadata columns**: Include `ingestion_timestamp`, `source_file` in bronze
5. **Use merge for updates**: Use `DeltaMergeWriter` for incremental loads
6. **Partition wisely**: Partition by date columns for time-series data
7. **Keep pipelines focused**: One pipeline per source-to-target transformation
8. **Use transforms**: Leverage `apply_schema` for consistent schema enforcement
9. **Register outputs**: Always call `self.outputs.add()` with matching alias
10. **Test locally**: Use local environment before deploying to Databricks

---

## Troubleshooting

### Reader not finding data
- Verify volume path is correct
- Check file exists at specified path
- Ensure file format matches definition

### Writer failing
- Check target schema matches DataFrame
- Verify catalog/schema exists
- Ensure write permissions

### Merge conflicts
- Verify primary_keys are unique in source
- Check merge conditions are correct
- Review merge_options configuration

### Schema mismatch
- Use `apply_schema` transform
- Verify schema field types match
- Check nullable settings
