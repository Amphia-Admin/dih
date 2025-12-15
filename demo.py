#!/usr/bin/env python
"""Run the orders pipeline.

This script demonstrates a complete medallion architecture pipeline
for orders data, processing data through Bronze → Silver → Gold layers
using the DIH framework with loadcore integration.

The pipeline processes two batches of order data:
- Batch 1: Initial load with 5 orders
- Batch 2: Updates (status changes) + 2 new orders

This demonstrates:
- CSV ingestion to Delta Lake
- Delta merge operations with partition locking
- Data deduplication and cleansing
- Business metric aggregations
- Loadcore integration for catalog and secrets management
"""

from src.dih.core.runner import Runner
from loadcore.src.constructor import Configuration
from examples.pipelines.orders.landed_to_bronze_orders_pipeline_transactions import LandedToBronzeOrdersBatch1Transactions
from pathlib import Path


_spark, catalog_name = Configuration().execute()

# =========================================================================
# Orders Pipeline - Landed to Bronze
# =========================================================================
run_cfg = {
    "root_path": Path(__file__).parent / "examples/pipelines/orders",
    "metadata": {
        "name": "orders_pipeline",
        "version": "1.0",
        "domain": "e-commerce",
    },
    "static_config": {},
    "spark_conf": {
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
        "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
    },
    "catalog": catalog_name,
}

runner = Runner(run_config=run_cfg, pipeline=LandedToBronzeOrdersBatch1Transactions)
runner.run()





print("\n[2/5] Running Silver layer - Batch 1...")
runner = Runner(run_cfg, SilverOrdersTransformation)
runner.run()
print("✓ Silver layer complete")

# =========================================================================
# Batch 2: Updates (status changes + new orders)
# =========================================================================

print("\n[3/5] Running Bronze layer - Batch 2 (updates)...")
runner = Runner(run_cfg, BronzeOrdersBatch2Transformation)
runner.run()
print("✓ Bronze layer complete")

print("\n[4/5] Running Silver layer - Batch 2 (merge updates)...")
runner = Runner(run_cfg, SilverOrdersTransformation)
runner.run()
print("✓ Silver layer complete")

# =========================================================================
# Gold Layer: Aggregations
# =========================================================================

print("\n[5/5] Running Gold layer (aggregations)...")
runner = Runner(run_cfg, GoldDailySalesTransformation)
runner.run()
print("✓ Gold layer complete")

