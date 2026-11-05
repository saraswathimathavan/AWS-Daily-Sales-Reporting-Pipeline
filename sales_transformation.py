
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    countDistinct,
    round
)

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("DailySalesReporting") \
    .getOrCreate()

# 2. Define S3 locations
input_path = "s3://sales-reporting-pipeline-2026-ishu/staging/overnight_sales.csv"
output_path = "s3://sales-reporting-pipeline-2026-ishu/presentation/"

print("Starting Daily Sales Reporting Pipeline")

# 3. Read the staging CSV
sales_df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

print("Staging data loaded")

# 4. Calculate sales for each order
sales_df = sales_df.withColumn(
    "order_sales",
    col("quantity") * col("unit_price")
)

# 5. Build the transformation DAG
metrics_df = sales_df.agg(
    spark_sum("order_sales").alias("total_sales"),
    countDistinct("order_id").alias("total_orders")
)

# 6. Calculate average order value
metrics_df = metrics_df.withColumn(
    "average_order_value",
    round(
        col("total_sales") / col("total_orders"),
        2
    )
)

# 7. Demonstrate lazy evaluation
print("\n========== DAG ANALYSIS ==========")

metrics_df.explain(extended=True)

print("\n========== EXECUTION STARTED ==========")

# 8. Trigger actual Spark execution
result = metrics_df.collect()[0]

total_sales = result["total_sales"]
total_orders = result["total_orders"]
average_order_value = result["average_order_value"]

print("\n========== SALES REPORT ==========")
print("Total Orders:", total_orders)
print("Total Sales:", total_sales)
print("Average Order Value:", average_order_value)

# 9. Write results to presentation layer
metrics_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(output_path)

print("\nReport successfully saved to:", output_path)

spark.stop()
