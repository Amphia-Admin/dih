# SRC - Pipeline Framework

The core framework for building medallion architecture data pipelines using a declarative, decorator-based approach.

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
│        └────────────┤           ├────────────┘                             │
│                     └───────────┘                                          │
│                                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Quick Start

A complete pipeline requires three parts: **definitions**, **schema**, and **pipeline logic**.

```python
# 1. Source Definition - where to read from
class LandedOrdersDef(TableDefinition):
    @property
    def path(self) -> str:
        return f"{self.get_volume('lake')}/raw/orders.csv"

    @property
    def format(self) -> str:
        return "csv"

    @property
    def options(self) -> dict:
        return {"header": "true", "inferSchema": "true"}


# 2. Target Definition - where to write to
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


# 3. Pipeline - the transformation logic
@pipeline_definition(name="BronzeOrdersPipeline")
@register_reader(definition=LandedOrdersDef, reader=SparkDataFrameReader, alias="orders")
@register_writer(definition=BronzeOrdersDef, writer=SparkDataFrameWriter, alias="bronze_orders")
class BronzeOrdersPipeline(Pipeline):
    def process(self) -> None:
        df = self.inputs["orders"]
        df = df.withColumn("ingestion_ts", F.current_timestamp())
        self.outputs.add("bronze_orders", df)


# 4. Run it
env = Environment("./env.config.yaml")
config = env.for_pipeline(metadata={"name": "orders"})
Runner(config=config, pipeline=BronzeOrdersPipeline).run()
```

## Core Components

### Runner

Orchestrates the ETL lifecycle: read → process → write.

```python
runner = Runner(config=pipeline_config, pipeline=MyPipeline)
runner.run()
```

### Pipeline

Base class for your ETL logic. Access inputs via `self.inputs["alias"]`, register outputs via `self.outputs.add("alias", df)`.

```python
@pipeline_definition(name="MyPipeline")
class MyPipeline(Pipeline):
    def process(self) -> None:
        df = self.inputs["source"]
        # transform...
        self.outputs.add("target", df)
```

### Table Definitions

Define where data lives and how to read/write it.

**Source tables** - implement `TableDefinition`:

| Property | Required | Description |
|----------|----------|-------------|
| `path` | Yes* | File path (use `get_volume()` for portability) |
| `format` | Yes | `csv`, `parquet`, `json`, `delta`, `avro`, `orc` |
| `table_name` | No | Catalog table name (alternative to path) |
| `options` | No | Reader options dict |
| `schema` | No | PySpark StructType |

**Target tables** - add `TargetTableDefMixin`:

| Property | Required | Description |
|----------|----------|-------------|
| `managed` | Yes | `True` = catalog table, `False` = path-based |
| `write_mode` | Yes | `append`, `overwrite`, `merge` |
| `partition_by` | No | List of partition columns |
| `primary_keys` | No | Business keys for merge operations |
| `merge_options` | No | Delta merge configuration |

## Readers

| Reader | Use Case |
|--------|----------|
| `SparkDataFrameReader` | Files: CSV, Parquet, JSON, Delta, Avro, ORC |
| `DeltaTableReader` | Delta tables (path or catalog) |
| `CatalogTableReader` | Unity Catalog tables |
| `AutoLoaderReader` | Incremental file ingestion (Databricks) |

```python
@register_reader(definition=MySourceDef, reader=SparkDataFrameReader, alias="source")
```

## Writers

| Writer | Use Case |
|--------|----------|
| `SparkDataFrameWriter` | Append/overwrite operations |
| `DeltaMergeWriter` | Upsert with business keys |
| `DeltaMergeAutoPartitionWriter` | Partition-optimised merge |

```python
@register_writer(definition=MyTargetDef, writer=SparkDataFrameWriter, alias="target")
```

### Merge Configuration

For `DeltaMergeWriter`, set `primary_keys` and optionally `merge_options`:

```python
@property
def primary_keys(self) -> list[str]:
    return ["order_id"]

@property
def merge_options(self) -> dict:
    return {
        "when_matched_update_condition": "src.updated_at > tgt.updated_at",
        "when_matched_delete_condition": "src.is_deleted = true",
        "source_alias": "src",
        "target_alias": "tgt",
    }
```

## Transforms

### apply_schema

Enforces a target schema: adds missing columns, casts types, drops extras, reorders.

```python
from src.transforms.apply_schema import apply_schema

df = apply_schema(raw_df, TARGET_SCHEMA)
```

## Onboarding a New Pipeline

### 1. Create definitions

```
pipelines/bronze/definitions/
├── landed_orders_def.py    # Source: where to read
└── bronze_orders_def.py    # Target: where to write
```

### 2. Create schema (recommended)

```
pipelines/bronze/schemas/
└── bronze_orders_schema.py
```

### 3. Create pipeline

```
pipelines/bronze/pipelines/
└── bronze_orders_pipeline.py
```

### 4. Export in `__init__.py`

```python
# pipelines/bronze/definitions/__init__.py
from .landed_orders_def import LandedOrdersDef
from .bronze_orders_def import BronzeOrdersDef
```

## Medallion Architecture

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
│ Landed  │────>│ Bronze  │────>│ Silver  │────>│  Gold   │
│  (Raw)  │     │(Cleansed)│    │(Conform)│     │(Business)│
└─────────┘     └─────────┘     └─────────┘     └─────────┘
```

| Layer | Purpose |
|-------|---------|
| **Landed** | Raw source data, minimal transformation |
| **Bronze** | Cleansed, de-duplicated, metadata added |
| **Silver** | Business logic, joins, conformance |
| **Gold** | Aggregated, business-ready datasets |

## Best Practices

1. **Use `get_volume()`** - Never hardcode paths
2. **Use managed tables** - Prefer catalog-managed Delta tables
3. **Define schemas** - Explicit schemas prevent surprises
4. **Add metadata** - Include `ingestion_timestamp`, `source_file` in bronze
5. **One pipeline per transformation** - Keep pipelines focused

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Reader not finding data | Check volume path, verify file exists |
| Writer failing | Verify catalog/schema exists, check permissions |
| Merge conflicts | Ensure primary_keys are unique in source |
| Schema mismatch | Use `apply_schema` transform |
