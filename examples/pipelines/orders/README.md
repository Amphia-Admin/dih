# Orders Pipeline

A complete example demonstrating the DIH framework's medallion architecture pattern for processing e-commerce order data through Bronze → Silver → Gold layers.

## Overview

This pipeline showcases:

- **Bronze Layer**: Raw data ingestion from CSV sources with minimal transformation
- **Silver Layer**: Data cleansing, deduplication, and Delta merge operations
- **Gold Layer**: Business-ready aggregated metrics for analytics

## Pipeline Architecture

```
CSV Source Data (data/)
    ├── batch1.csv (Initial load: 5 orders)
    └── batch2.csv (Updates: 2 status changes + 2 new orders)
         ↓
Bronze Layer (lakehouse/bronze/orders/)
    - Format: Delta Lake
    - Mode: Append (preserves all batches)
    - Transformations: Add ingestion_timestamp
         ↓
Silver Layer (lakehouse/silver/orders/)
    - Format: Delta Lake
    - Mode: Merge (upsert on order_id)
    - Partitions: year, month
    - Transformations: Deduplication, calculated columns
         ↓
Gold Layer (lakehouse/gold/daily_sales/)
    - Format: Delta Lake
    - Mode: Overwrite (full refresh)
    - Metrics: Daily sales aggregations
```

## File Organization

```
orders/
├── data/                    # Source CSV files
│   ├── batch1.csv          # Initial orders data
│   └── batch2.csv          # Update batch with status changes
│
├── table_definitions.py    # All table definitions for orders entity
│   ├── SourceOrdersBatch1  # CSV source for batch 1
│   ├── SourceOrdersBatch2  # CSV source for batch 2
│   ├── BronzeOrders        # Bronze layer Delta table
│   ├── SilverOrders        # Silver layer Delta table
│   └── GoldDailySales      # Gold layer Delta table
│
├── bronze.py               # Bronze layer transformations
│   ├── BronzeOrdersBatch1Transformation
│   └── BronzeOrdersBatch2Transformation
│
├── silver.py               # Silver layer transformation
│   └── SilverOrdersTransformation
│
├── gold.py                 # Gold layer transformations
│   └── GoldDailySalesTransformation
│
├── config.py               # Pipeline configuration
│   ├── PipelineConfig
│   └── configure_spark_with_delta_pip()
│
├── run.py                  # Main orchestration script
└── README.md              # This file
```

## Running the Pipeline

### Prerequisites

```bash
# Ensure DIH is installed in development mode
pip install -e .

# Install dependencies (if not already installed)
pip install pyspark delta-spark pandas
```

### Execution

From the repository root:

```bash
python -m examples.pipelines.orders.run
```

Or navigate to the orders directory:

```bash
cd examples/pipelines/orders
python run.py
```

### Expected Output

```
================================================================================
DIH Framework - Orders Pipeline
================================================================================

[1/5] Running Bronze layer - Batch 1 (initial load)...
✓ Bronze layer complete

[2/5] Running Silver layer - Batch 1...
✓ Silver layer complete

[3/5] Running Bronze layer - Batch 2 (updates)...
✓ Bronze layer complete

[4/5] Running Silver layer - Batch 2 (merge updates)...
✓ Silver layer complete

[5/5] Running Gold layer (aggregations)...
✓ Gold layer complete

================================================================================
Pipeline Complete! Results:
================================================================================

Bronze Orders (all batches appended):
Total records in bronze: 9

Silver Orders (deduplicated, merged):
Total records in silver: 7

Gold Daily Sales (aggregated metrics):
+----------+------------+-------------+-----------------+
|order_date|total_orders|total_revenue|unique_customers |
+----------+------------+-------------+-----------------+
|2024-01-15|1           |49.99        |1                |
|2024-01-16|1           |59.97        |1                |
|2024-01-17|1           |79.98        |1                |
|2024-01-18|1           |19.99        |1                |
+----------+------------+-------------+-----------------+

✓ Pipeline validation PASSED!
```

## Data Schema

### Source CSV Schema

```
order_id     : string  - Unique order identifier (business key)
customer_id  : string  - Customer identifier
product_id   : string  - Product identifier
quantity     : integer - Number of items ordered
price        : decimal - Unit price
order_date   : date    - Date of order
status       : string  - Order status (pending, completed, cancelled)
```

### Bronze Layer Schema

Source schema + metadata:

```
ingestion_timestamp : timestamp - When the record was ingested
```

### Silver Layer Schema

Bronze schema + derived columns:

```
year         : integer - Extracted from order_date (for partitioning)
month        : integer - Extracted from order_date (for partitioning)
total_amount : decimal - Calculated as quantity * price
```

### Gold Layer Schema

Aggregated metrics:

```
order_date        : date    - Aggregation date
total_orders      : long    - Count of completed orders
total_revenue     : decimal - Sum of total_amount for completed orders
unique_customers  : long    - Distinct count of customer_id
```

## Key Features Demonstrated

### 1. TableDefinition Pattern

All table configurations are centralized in `table_definitions.py`:

```python
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

### 2. Decorator-Based Registration

Transformations declare their inputs and outputs using decorators:

```python
@transformation_definition(name="silver_orders")
@register_reader(BronzeOrders, SparkDataFrameReader, alias="bronze_orders")
@register_writer(SilverOrders, DeltaMergeAutoPartitionWriter, alias="silver_orders")
class SilverOrdersTransformation(Transformation):
    def process(self) -> None:
        df = self.inputs["bronze_orders"]
        # ... transformation logic ...
        self.outputs.add("silver_orders", df_clean)
```

### 3. Delta Merge with Partition Locking

`DeltaMergeAutoPartitionWriter` automatically:

- Detects partition values from the DataFrame (year, month)
- Locks only affected partitions for performance
- Merges based on primary key (order_id)
- Updates existing records, inserts new ones

### 4. Deduplication Strategy

Silver layer handles duplicate order_ids from multiple batches:

```python
# Keep latest record per order_id based on ingestion_timestamp
window_spec = Window.partitionBy("order_id").orderBy(F.col("ingestion_timestamp").desc())
df_deduped = (
    df.withColumn("row_num", F.row_number().over(window_spec))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
```

### 5. Runner Orchestration

Simple execution model with automatic I/O handling:

```python
run_config = {"root_path": str(pipeline_root)}
runner = Runner(run_config, SilverOrdersTransformation)
runner.run()
```

## Data Flow Example

### Batch 1 (Initial Load)

**batch1.csv:**
```csv
order_id,customer_id,product_id,quantity,price,order_date,status
1001,C001,P100,2,29.99,2024-01-15,pending
1002,C002,P101,1,49.99,2024-01-15,completed
```

**Bronze:** Appends 5 records with `ingestion_timestamp`

**Silver:** Inserts 5 deduplicated records with calculated `total_amount`, `year`, `month`

**Gold:** Aggregates completed orders by date

### Batch 2 (Updates)

**batch2.csv:**
```csv
order_id,customer_id,product_id,quantity,price,order_date,status
1001,C001,P100,2,29.99,2024-01-15,completed  # Status change: pending → completed
1004,C003,P100,1,29.99,2024-01-16,cancelled  # Status change: pending → cancelled
1006,C004,P101,2,49.99,2024-01-18,pending    # New order
```

**Bronze:** Appends 4 more records (total: 9 in bronze)

**Silver:**
- Merges updates for orders 1001, 1004 (status changes)
- Inserts new orders 1006, 1007
- Result: 7 unique orders in silver

**Gold:**
- Recalculates daily metrics
- Order 1001 now contributes to completed revenue
- Order 1004 excluded (cancelled status)

## Cleanup

To remove generated lakehouse data:

```bash
rm -rf examples/pipelines/orders/lakehouse
```

## Extending This Pipeline

### Add New Gold Layer Metrics

Create additional transformations in `gold.py`:

```python
@transformation_definition(name="gold_customer_metrics")
@register_reader(SilverOrders, SparkDataFrameReader, alias="silver_orders")
@register_writer(GoldCustomerMetrics, SparkDataFrameWriter, alias="customer_metrics")
class GoldCustomerMetricsTransformation(Transformation):
    def process(self) -> None:
        # Calculate customer lifetime value, order frequency, etc.
        pass
```

### Add Product Dimension

Create new pipeline under `examples/pipelines/products/` following the same structure.

### Add Incremental Processing

Modify silver transformation to use watermarks or processing timestamps for incremental reads from bronze.

## Troubleshooting

### ModuleNotFoundError: No module named 'dih'

Ensure DIH is installed:
```bash
pip install -e .
```

### Delta merge errors

Check that:
- `primary_keys` is defined in the target TableDefinition
- DataFrame contains all partition columns before merge
- No duplicate rows exist after deduplication

### Path issues

Verify `root_path` in run configuration points to `examples/pipelines/orders`.
