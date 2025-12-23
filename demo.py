#!/usr/bin/env python
"""Run the products pipeline.

This script demonstrates a complete medallion architecture pipeline
for products data using the DIH framework with loadcore integration.
"""

from examples.pipelines.products.landed_to_bronze_products_pipeline import (
    LandedToBronzeProductsPipeline,
)
from loadcore.environment import Environment
from src.dih.core.runner import Runner

# Initialize environment (auto-detects local vs remote)
env = Environment("./env.config.yaml")

# Create pipeline config
config = env.for_pipeline(
    metadata={
        "name": "products_pipeline",
        "version": "1.0",
        "domain": "e-commerce",
    },
    spark_conf={
        "spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite": "true",
        "spark.databricks.delta.properties.defaults.autoOptimize.autoCompact": "true",
    },
)

# Run the pipeline
runner = Runner(config=config, pipeline=LandedToBronzeProductsPipeline)
runner.run()
