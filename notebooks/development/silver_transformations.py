
from pyspark.sql import functions as F

BRONZE = "de_dev.bronze"
SILVER = "de_dev.silver"
QUARANTINE = "de_dev.quarantine"

# Ensure target schemas exist.
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {QUARANTINE}")

# ---------------------------------------------------------
# 1. Orders: validate required keys and timestamps
# ---------------------------------------------------------
orders = spark.table(f"{BRONZE}.olist_orders")

orders_checked = orders.withColumn(
    "_rejection_reason",
    F.when(F.col("order_id").isNull(), "missing_order_id")
     .when(F.col("customer_id").isNull(), "missing_customer_id")
     .when(F.col("order_purchase_timestamp").isNull(),
           "missing_purchase_timestamp")
)

invalid_orders = orders_checked.filter(
    F.col("_rejection_reason").isNotNull()
)

valid_orders = orders_checked.filter(
    F.col("_rejection_reason").isNull()
)

# Quarantine rejected orders, retaining source metadata.
quarantine_orders = invalid_orders.select(
    *orders.columns,
    "_rejection_reason"
)

(
    quarantine_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{QUARANTINE}.olist_orders")
)

# Keep only valid orders and standardise selected fields.
silver_orders = (
    valid_orders
    .drop("_rejection_reason", "_rescued_data")
    .withColumn("order_status", F.lower(F.trim("order_status")))
    .withColumn("customer_id", F.trim("customer_id"))
)

(
    silver_orders.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.olist_orders")
)

# ---------------------------------------------------------
# 2. Order items: validate keys and monetary values
# ---------------------------------------------------------
items = spark.table(f"{BRONZE}.olist_order_items")

items_checked = items.withColumn(
    "_rejection_reason",
    F.when(F.col("order_id").isNull(), "missing_order_id")
     .when(F.col("order_item_id").isNull(), "missing_order_item_id")
     .when(F.col("product_id").isNull(), "missing_product_id")
     .when(F.col("seller_id").isNull(), "missing_seller_id")
     .when(F.col("price").isNull(), "missing_price")
     .when(F.col("price") < 0, "negative_price")
     .when(F.col("freight_value").isNull(), "missing_freight_value")
     .when(F.col("freight_value") < 0, "negative_freight_value")
)

invalid_items = items_checked.filter(
    F.col("_rejection_reason").isNotNull()
)

valid_items = items_checked.filter(
    F.col("_rejection_reason").isNull()
)

(
    invalid_items.select(*items.columns, "_rejection_reason")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{QUARANTINE}.olist_order_items")
)

silver_items = (
    valid_items
    .drop("_rejection_reason", "_rescued_data")
    .withColumn("price", F.col("price").cast("decimal(12,2)"))
    .withColumn("freight_value", F.col("freight_value").cast("decimal(12,2)"))
)

(
    silver_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.olist_order_items")
)

# ---------------------------------------------------------
# 3. Customers: validate and standardise customer fields
# ---------------------------------------------------------
customers = spark.table(f"{BRONZE}.olist_customers")

customers_checked = customers.withColumn(
    "_rejection_reason",
    F.when(F.col("customer_id").isNull(), "missing_customer_id")
     .when(F.col("customer_unique_id").isNull(), "missing_customer_unique_id")
)

invalid_customers = customers_checked.filter(
    F.col("_rejection_reason").isNotNull()
)

valid_customers = customers_checked.filter(
    F.col("_rejection_reason").isNull()
)

(
    invalid_customers.select(*customers.columns, "_rejection_reason")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{QUARANTINE}.olist_customers")
)

silver_customers = (
    valid_customers
    .drop("_rejection_reason", "_rescued_data")
    .withColumn("customer_city", F.lower(F.trim("customer_city")))
    .withColumn("customer_state", F.upper(F.trim("customer_state")))
)

(
    silver_customers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.olist_customers")
)

# ---------------------------------------------------------
# 4. Report data quality results
# ---------------------------------------------------------
for table_name in ["olist_orders", "olist_order_items", "olist_customers"]:
    silver_count = spark.table(f"{SILVER}.{table_name}").count()
    quarantine_count = spark.table(f"{QUARANTINE}.{table_name}").count()

    print(
        f"{table_name}: "
        f"Silver rows={silver_count}, "
        f"Quarantined rows={quarantine_count}"
    )

print("Initial Silver transformations completed.")
