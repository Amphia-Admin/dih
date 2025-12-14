"""Gold layer transformation - business-ready aggregated metrics.

This module contains transformations for creating business-ready aggregated
metrics from silver layer data. Gold layer tables are designed for direct
consumption by analytics and reporting tools.
"""

from pyspark.sql import functions as F

from dih import (
    SparkDataFrameReader,
    SparkDataFrameWriter,
    Transformation,
    register_reader,
    register_writer,
    transformation_definition,
)
from examples.pipelines.orders.table_definitions import GoldDailySales, SilverOrders


@transformation_definition(name="gold_daily_sales")
@register_reader(SilverOrders, SparkDataFrameReader, alias="silver_orders")
@register_writer(GoldDailySales, SparkDataFrameWriter, alias="gold_sales")
class GoldDailySalesTransformation(Transformation):
    """
    Gold layer transformation for daily sales metrics.

    Aggregates silver layer data to produce business-ready metrics
    for daily sales performance analysis. Only includes completed
    orders in revenue calculations to ensure accuracy.

    Metrics produced:
    - total_orders: Count of completed orders per day
    - total_revenue: Sum of order amounts for completed orders
    - unique_customers: Distinct customer count per day
    """

    def process(self) -> None:
        """
        Process silver data to gold layer.

        Aggregations performed:
        - Total orders per day (count of order_id)
        - Total revenue per day (sum of total_amount for completed orders)
        - Unique customers per day (distinct count of customer_id)

        Business Rules
        --------------
        Only orders with status='completed' are included in the aggregations
        to ensure revenue metrics reflect actual completed transactions.

        Orders with other statuses (pending, cancelled) are excluded from
        the daily sales metrics but could be used in other gold layer
        aggregations (e.g., cancellation rate analysis).
        """
        # Read from silver layer
        df = self.inputs["silver_orders"]

        # Aggregate by order_date
        # Only include completed orders for revenue calculation
        daily_sales = (
            df.filter(F.col("status") == "completed")
            .groupBy("order_date")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("total_revenue"),
                F.countDistinct("customer_id").alias("unique_customers"),
            )
            .orderBy("order_date")
        )

        # Write to gold layer (overwrite mode)
        # Full refresh ensures metrics are always current
        self.outputs.add("gold_sales", daily_sales)
