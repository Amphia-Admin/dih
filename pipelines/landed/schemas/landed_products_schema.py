"""Schema for landed products data."""

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

LANDED_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("category", StringType(), nullable=True),
    StructField("price", DecimalType(10, 2), nullable=True),
    StructField("stock_quantity", IntegerType(), nullable=True),
    StructField("supplier_id", StringType(), nullable=True),
])
