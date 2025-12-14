# DIH Framework Examples

This directory contains example pipelines demonstrating the DIH (Data Ingestion Hub) framework's capabilities for building production-ready data pipelines using the medallion architecture pattern.

## Quick Start

```bash
# Install DIH in development mode
pip install -e .

# Run the orders pipeline example
python -m examples.pipelines.orders.run
```

## Available Examples

### 1. Orders Pipeline (`pipelines/orders/`)

A complete end-to-end example demonstrating the medallion architecture for e-commerce order data.

**What it demonstrates:**
- CSV to Delta Lake ingestion
- Bronze → Silver → Gold data flow
- Delta merge operations with partition locking
- Data deduplication strategies
- Business metric aggregations

**Pipeline layers:**
- **Bronze**: Raw CSV ingestion with metadata tracking
- **Silver**: Cleaned, deduplicated data with Delta merges
- **Gold**: Daily sales metrics and analytics

**Key features:**
- `DeltaMergeAutoPartitionWriter` for optimized upserts
- Window functions for deduplication
- Partition-based performance optimization
- Multi-batch processing (initial load + updates)

**Run it:**
```bash
python -m examples.pipelines.orders.run
```

See [`pipelines/orders/README.md`](pipelines/orders/README.md) for detailed documentation.

## Example Organization Pattern

Each pipeline follows an entity-first organization structure, bundling all components for a specific business entity together:

```
examples/
└── pipelines/
    └── {entity}/              # e.g., orders, customers, products
        ├── data/              # Source data files (CSV, JSON, etc.)
        ├── table_definitions.py  # All table definitions
        ├── bronze.py          # Bronze layer transformations
        ├── silver.py          # Silver layer transformations
        ├── gold.py            # Gold layer transformations
        ├── config.py          # Pipeline configuration
        ├── run.py             # Orchestration script
        └── README.md          # Pipeline documentation
```

### Benefits of Entity-First Organization

**1. Colocation of Related Code**
- All components for one entity are in one place
- Easy to find and modify entity-specific logic
- Clear ownership and boundaries

**2. Natural Layer Progression**
- Bronze → Silver → Gold flow is explicit in filenames
- Easy to trace data lineage within an entity
- Simplifies debugging and maintenance

**3. Independent Evolution**
- Each pipeline evolves independently
- No cross-entity coupling or dependencies
- Safe to add/remove/modify pipelines

**4. Clear Testing Scope**
- Test data lives with the pipeline
- Integration tests are self-contained
- Easy to run individual pipelines in isolation

## Core DIH Concepts

### TableDefinition

Centralized configuration for all data sources and targets:

```python
from dih import TableDefinition, TargetTableDefMixin, PathBuilder, LakeLayer

class SilverOrders(TableDefinition, TargetTableDefMixin):
    @property
    def path(self) -> str:
        return PathBuilder(self.root_path).layer(LakeLayer.SILVER).table("orders").build()

    @property
    def primary_keys(self) -> list[str]:
        return ["order_id"]

    @property
    def partition_by(self) -> list[str]:
        return ["year", "month"]
```

### Transformation with Decorators

Declare inputs and outputs using decorator pattern:

```python
from dih import (
    Transformation,
    transformation_definition,
    register_reader,
    register_writer,
    SparkDataFrameReader,
    DeltaMergeAutoPartitionWriter,
)

@transformation_definition(name="silver_orders")
@register_reader(BronzeOrders, SparkDataFrameReader, alias="bronze_orders")
@register_writer(SilverOrders, DeltaMergeAutoPartitionWriter, alias="silver_orders")
class SilverOrdersTransformation(Transformation):
    def process(self) -> None:
        df = self.inputs["bronze_orders"]
        # ... transformation logic ...
        self.outputs.add("silver_orders", df_clean)
```

### Runner Orchestration

Simple execution model with automatic I/O handling:

```python
from dih import Runner

run_config = {"root_path": "/path/to/pipeline"}
runner = Runner(run_config, SilverOrdersTransformation)
result = runner.run()
```

## Framework Features Demonstrated

### Data Ingestion

- **CSV Sources**: `SparkDataFrameReader` with configurable options
- **Delta Tables**: Native Delta Lake integration
- **Path Management**: `PathBuilder` for consistent path construction

### Data Transformation

- **Bronze Layer**: Raw data landing with minimal transformation
- **Silver Layer**: Cleansing, deduplication, type casting
- **Gold Layer**: Business metrics and aggregations

### Delta Lake Operations

- **Append Mode**: Preserve complete history in Bronze
- **Merge Mode**: Upsert operations in Silver using business keys
- **Overwrite Mode**: Full refresh for aggregated Gold tables
- **Partition Locking**: Automatic optimization with `DeltaMergeAutoPartitionWriter`

### Type Safety

- Strongly typed table definitions
- Type-checked transformations
- Mypy-compliant codebase

### Observability

- Processing metadata tracking
- Data lineage through ingestion timestamps
- Validation and verification built into examples

## Creating Your Own Pipeline

### 1. Create Pipeline Directory

```bash
mkdir -p examples/pipelines/my_entity/data
```

### 2. Define Tables

Create `examples/pipelines/my_entity/table_definitions.py`:

```python
from dih import TableDefinition, TargetTableDefMixin, PathBuilder, LakeLayer

class SourceMyData(TableDefinition):
    @property
    def path(self) -> str:
        return str(Path(self.root_path) / "data" / "my_data.csv")

    @property
    def format(self) -> str:
        return "csv"

class BronzeMyEntity(TableDefinition, TargetTableDefMixin):
    @property
    def path(self) -> str:
        return PathBuilder(self.root_path).layer(LakeLayer.BRONZE).table("my_entity").build()

    @property
    def format(self) -> str:
        return "delta"
```

### 3. Create Transformations

Create `examples/pipelines/my_entity/bronze.py`:

```python
from dih import (
    Transformation,
    transformation_definition,
    register_reader,
    register_writer,
    SparkDataFrameReader,
    SparkDataFrameWriter,
)
from .table_definitions import SourceMyData, BronzeMyEntity

@transformation_definition(name="bronze_my_entity")
@register_reader(SourceMyData, SparkDataFrameReader, alias="source")
@register_writer(BronzeMyEntity, SparkDataFrameWriter, alias="bronze")
class BronzeMyEntityTransformation(Transformation):
    def process(self) -> None:
        df = self.inputs["source"]
        # Add transformations here
        self.outputs.add("bronze", df)
```

### 4. Create Run Script

Create `examples/pipelines/my_entity/run.py`:

```python
from dih import Runner
from pyspark.sql import SparkSession
from .bronze import BronzeMyEntityTransformation

def main():
    spark = SparkSession.builder.appName("MyPipeline").getOrCreate()
    run_config = {"root_path": "/path/to/pipeline"}
    runner = Runner(run_config, BronzeMyEntityTransformation)
    runner.run()
    spark.stop()

if __name__ == "__main__":
    main()
```

### 5. Document Your Pipeline

Create `examples/pipelines/my_entity/README.md` documenting:
- Pipeline purpose and business context
- Data schema and transformations
- How to run the pipeline
- Expected outputs and validation

## Testing Examples

All examples are designed to be self-contained and runnable. Use them as:

### Integration Tests

Run examples as part of your CI/CD pipeline:

```bash
# Run all examples
for pipeline in examples/pipelines/*/; do
    python -m "$(echo $pipeline | tr '/' '.').run"
done
```

### Learning Resources

Study the examples to understand:
- Best practices for organizing pipelines
- Common transformation patterns
- Error handling and validation
- Performance optimization techniques

### Templates

Copy an example as a starting point for your own pipelines:

```bash
cp -r examples/pipelines/orders examples/pipelines/my_new_pipeline
# Modify files to suit your use case
```

## Best Practices

### 1. Separate Source and Target Definitions

- Source definitions point to external data (CSV, APIs, databases)
- Target definitions use `TargetTableDefMixin` for Delta tables
- Keep them in the same `table_definitions.py` for easy reference

### 2. Use PathBuilder Consistently

```python
# Good: Consistent path construction
PathBuilder(root_path).layer(LakeLayer.SILVER).table("orders").build()

# Avoid: Manual path construction
f"{root_path}/silver/orders"  # Fragile, inconsistent
```

### 3. Leverage Partition Locking

For large Silver tables, use `DeltaMergeAutoPartitionWriter` with partitions:

```python
@property
def partition_by(self) -> list[str]:
    return ["year", "month"]  # Or whatever makes sense for your data
```

### 4. Document Business Logic

Add docstrings explaining:
- Why transformations are done (business context)
- What edge cases are handled
- How to interpret output metrics

### 5. Include Validation

Each pipeline's run script should verify expected outcomes:

```python
# Verify record counts
assert silver_df.count() == expected_count, "Unexpected record count in Silver"

# Verify no nulls in key columns
assert silver_df.filter(F.col("order_id").isNull()).count() == 0, "Null order_ids found"
```

## Troubleshooting

### Common Issues

**ModuleNotFoundError: No module named 'dih'**
```bash
# Solution: Install DIH in editable mode
pip install -e .
```

**Delta merge multiple source rows error**
```python
# Solution: Deduplicate before merge
window_spec = Window.partitionBy("id").orderBy(F.col("timestamp").desc())
df_deduped = df.withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num")
```

**Partition not found errors**
```python
# Solution: Ensure partition columns exist in DataFrame before merge
df = df.withColumn("year", F.year(F.col("date"))) \
       .withColumn("month", F.month(F.col("date")))
```

## Contributing Examples

When contributing new examples:

1. Follow the entity-first organization pattern
2. Include comprehensive README.md documentation
3. Add sample data in `data/` directory
4. Include validation in the run script
5. Ensure ruff and mypy compliance
6. Add NumPy-style docstrings to all functions/classes

## Resources

- [DIH Framework Documentation](../README.md)
- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Medallion Architecture Pattern](https://www.databricks.com/glossary/medallion-architecture)
