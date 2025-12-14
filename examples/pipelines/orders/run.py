#!/usr/bin/env python
"""Run the orders pipeline.

This script demonstrates a complete medallion architecture pipeline
for orders data, processing data through Bronze → Silver → Gold layers
using the DIH framework.

The pipeline processes two batches of order data:
- Batch 1: Initial load with 5 orders
- Batch 2: Updates (status changes) + 2 new orders

This demonstrates:
- CSV ingestion to Delta Lake
- Delta merge operations with partition locking
- Data deduplication and cleansing
- Business metric aggregations
"""

from pyspark.sql import SparkSession

from dih import Runner
from examples.pipelines.orders.bronze import (
    BronzeOrdersBatch1Transformation,
    BronzeOrdersBatch2Transformation,
)
from examples.pipelines.orders.config import (
    PipelineConfig,
    configure_spark_with_delta_pip,
)
from examples.pipelines.orders.gold import GoldDailySalesTransformation
from examples.pipelines.orders.silver import SilverOrdersTransformation


def main() -> None:
    """
    Execute the orders pipeline.

    Pipeline Execution Flow
    -----------------------
    1. Bronze Batch 1: Ingest initial orders from batch1.csv
    2. Silver Batch 1: Clean, deduplicate, and merge to silver layer
    3. Bronze Batch 2: Ingest updates from batch2.csv
    4. Silver Batch 2: Merge updates (demonstrates upsert behavior)
    5. Gold: Generate daily sales aggregations

    Results Verification
    --------------------
    After execution, the script displays data from each layer and
    validates expected record counts to ensure pipeline correctness.
    """
    # Initialize configuration
    config = PipelineConfig()
    run_config = config.get_run_config()

    # Create Spark session with Delta Lake support
    builder = (
        SparkSession.builder.appName("OrdersPipeline")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("=" * 80)
    print("DIH Framework - Orders Pipeline")
    print("=" * 80)

    # =========================================================================
    # Batch 1: Initial Load
    # =========================================================================

    print("\n[1/5] Running Bronze layer - Batch 1 (initial load)...")
    runner = Runner(run_config, BronzeOrdersBatch1Transformation)
    runner.run()
    print("✓ Bronze layer complete")

    print("\n[2/5] Running Silver layer - Batch 1...")
    runner = Runner(run_config, SilverOrdersTransformation)
    runner.run()
    print("✓ Silver layer complete")

    # =========================================================================
    # Batch 2: Updates (status changes + new orders)
    # =========================================================================

    print("\n[3/5] Running Bronze layer - Batch 2 (updates)...")
    runner = Runner(run_config, BronzeOrdersBatch2Transformation)
    runner.run()
    print("✓ Bronze layer complete")

    print("\n[4/5] Running Silver layer - Batch 2 (merge updates)...")
    runner = Runner(run_config, SilverOrdersTransformation)
    runner.run()
    print("✓ Silver layer complete")

    # =========================================================================
    # Gold Layer: Aggregations
    # =========================================================================

    print("\n[5/5] Running Gold layer (aggregations)...")
    runner = Runner(run_config, GoldDailySalesTransformation)
    runner.run()
    print("✓ Gold layer complete")

    # =========================================================================
    # Display Results
    # =========================================================================

    print("\n" + "=" * 80)
    print("Pipeline Complete! Results:")
    print("=" * 80)

    print("\nBronze Orders (all batches appended):")
    bronze_df = spark.read.format("delta").load(
        str(config.lakehouse_root / "bronze" / "orders")
    )
    print(f"Total records in bronze: {bronze_df.count()}")
    bronze_df.select("order_id", "status", "order_date", "ingestion_timestamp").orderBy(
        "order_id"
    ).show(truncate=False)

    print("\nSilver Orders (deduplicated, merged):")
    silver_df = spark.read.format("delta").load(
        str(config.lakehouse_root / "silver" / "orders")
    )
    print(f"Total records in silver: {silver_df.count()}")
    silver_df.select(
        "order_id", "status", "order_date", "total_amount", "year", "month"
    ).orderBy("order_id").show(truncate=False)

    print("\nGold Daily Sales (aggregated metrics):")
    gold_df = spark.read.format("delta").load(
        str(config.lakehouse_root / "gold" / "daily_sales")
    )
    gold_df.show(truncate=False)

    # =========================================================================
    # Verification
    # =========================================================================

    print("\n" + "=" * 80)
    print("Verification:")
    print("=" * 80)
    print("\nExpected results:")
    print("- Bronze: 9 records (5 from batch1 + 4 from batch2)")
    print("- Silver: 7 unique orders (deduplicated)")
    print("  - Order 1001: status updated pending → completed")
    print("  - Order 1004: status updated pending → cancelled")
    print("  - Orders 1006, 1007: new orders")
    print("- Gold: 4 rows (daily aggregations for completed orders)")

    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    gold_count = gold_df.count()

    print("\nActual results:")
    print(f"- Bronze: {bronze_count} records")
    print(f"- Silver: {silver_count} records")
    print(f"- Gold: {gold_count} rows")

    if bronze_count == 9 and silver_count == 7:
        print("\n✓ Pipeline validation PASSED!")
    else:
        print("\n✗ Pipeline validation FAILED - unexpected record counts")

    spark.stop()
    print("\nSpark session stopped.")


if __name__ == "__main__":
    main()
