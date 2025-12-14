"""Configuration for orders pipeline.

This module contains configuration defaults for the orders data pipeline.
for automatic environment detection and catalog management.
"""

from pathlib import Path

# Pipeline paths
PIPELINE_ROOT = Path(__file__).parent
DATA_DIR = PIPELINE_ROOT / "data"
LAKEHOUSE_ROOT = PIPELINE_ROOT / "lakehouse"

# Default Spark configuration for Delta optimizations
DEFAULT_SPARK_CONF = {
    "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
    "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
}

# Pipeline metadata
PIPELINE_METADATA = {
    "name": "orders_pipeline",
    "version": "1.0",
    "domain": "e-commerce",
}
