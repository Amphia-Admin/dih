"""Orders pipeline - Medallion architecture example.

This package demonstrates a complete data pipeline using the DIH framework
to process e-commerce order data through Bronze → Silver → Gold layers.

Modules
-------
table_definitions
    All TableDefinition classes for orders pipeline.
bronze
    Bronze layer transformations for raw data ingestion.
silver
    Silver layer transformation for cleaned and deduplicated data.
gold
    Gold layer transformations for business metrics.
config
    Pipeline configuration and Spark setup utilities.
run
    Main orchestration script for executing the pipeline.

"""

__all__ = [
    "DEFAULT_SPARK_CONF",
    "LAKEHOUSE_ROOT",
    "PIPELINE_METADATA",
    "PIPELINE_ROOT",
    "BronzeOrders",
    "BronzeOrdersBatch1Transformation",
    "BronzeOrdersBatch2Transformation",
    "GoldDailySales",
    "GoldDailySalesTransformation",
    "SilverOrders",
    "SilverOrdersTransformation",
    "SourceOrdersBatch1",
    "SourceOrdersBatch2",
]

from examples.pipelines.orders.bronze import (
    BronzeOrdersBatch1Transformation,
    BronzeOrdersBatch2Transformation,
)
from examples.pipelines.orders.config import (
    DEFAULT_SPARK_CONF,
    LAKEHOUSE_ROOT,
    PIPELINE_METADATA,
    PIPELINE_ROOT,
)
from examples.pipelines.orders.gold import GoldDailySalesTransformation
from examples.pipelines.orders.silver import SilverOrdersTransformation
from examples.pipelines.orders.table_definitions import (
    BronzeOrders,
    GoldDailySales,
    SilverOrders,
    SourceOrdersBatch1,
    SourceOrdersBatch2,
)
