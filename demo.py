"""Run the products pipeline."""

from pipelines.bronze.pipelines.bronze_products_pipeline import BronzeProductsPipeline
from loadcore.environment import Environment
from src.core.runner import Runner

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
runner = Runner(config=config, pipeline=BronzeProductsPipeline)
runner.run()
