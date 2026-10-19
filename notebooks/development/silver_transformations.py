
# SILVER LAYER LOGIC
# - Schema enforcement and schema evolution
# - Data type casting and standardisation
# - Required-field and business-rule validation
# - Duplicate detection and deduplication
# - Data quality checks and quality gates
# - Referential integrity validation
# - Quarantine invalid records with rejection reasons
# - Rescued data handling
# - Source metadata and data lineage
# - Idempotent processing and incremental MERGE (planned)
# - CDC processing and SCD Type 1/Type 2 (planned)
# - Late-arriving data handling (planned)
# - Full-refresh overwrite for the current initial implementation


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
# 4. Products: validate and standardise
# ---------------------------------------------------------
products = spark.table(f"{BRONZE}.olist_products")

products_checked = products.withColumn(
    "_rejection_reason",
    F.when(F.col("product_id").isNull(), "missing_product_id")
)

invalid_products = products_checked.filter(
    F.col("_rejection_reason").isNotNull()
)

valid_products = products_checked.filter(
    F.col("_rejection_reason").isNull()
)

(
    invalid_products.select(*products.columns, "_rejection_reason")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{QUARANTINE}.olist_products")
)

silver_products = (
    valid_products
    .drop("_rejection_reason", "_rescued_data")
    .withColumn(
        "product_category_name",
        F.lower(F.trim("product_category_name"))
    )
)

(
    silver_products.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.olist_products")
)

# ---------------------------------------------------------
# 5. Sellers: validate and standardise
# ---------------------------------------------------------
sellers = spark.table(f"{BRONZE}.olist_sellers")

sellers_checked = sellers.withColumn(
    "_rejection_reason",
    F.when(F.col("seller_id").isNull(), "missing_seller_id")
)

invalid_sellers = sellers_checked.filter(
    F.col("_rejection_reason").isNotNull()
)

valid_sellers = sellers_checked.filter(
    F.col("_rejection_reason").isNull()
)

(
    invalid_sellers.select(*sellers.columns, "_rejection_reason")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{QUARANTINE}.olist_sellers")
)

silver_sellers = (
    valid_sellers
    .drop("_rejection_reason", "_rescued_data")
    .withColumn("seller_city", F.lower(F.trim("seller_city")))
    .withColumn("seller_state", F.upper(F.trim("seller_state")))
)

(
    silver_sellers.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.olist_sellers")
)




# ---------------------------------------------------------
# 6. Payments: validate and standardise payment fields
# ---------------------------------------------------------
payments = spark.table(f"{BRONZE}.olist_order_payments")

payments_checked = payments.withColumn(
    "_rejection_reason",
    F.when(F.col("order_id").isNull(), "missing_order_id")
     .when(F.col("payment_sequential").isNull(), "missing_payment_sequential")
     .when(F.col("payment_type").isNull(), "missing_payment_type")
     .when(F.col("payment_value").isNull(), "missing_payment_value")
     .when(F.col("payment_value") < 0, "negative_payment_value")
     .when(F.col("payment_installments").isNull(), "missing_payment_installments")
     .when(F.col("payment_installments") < 0, "negative_payment_installments")
)

invalid_payments = payments_checked.filter(
    F.col("_rejection_reason").isNotNull()
)

valid_payments = payments_checked.filter(
    F.col("_rejection_reason").isNull()
)

(
    invalid_payments.select(*payments.columns, "_rejection_reason")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{QUARANTINE}.olist_order_payments")
)

silver_payments = (
    valid_payments
    .drop("_rejection_reason", "_rescued_data")
    .withColumn("payment_type", F.lower(F.trim("payment_type")))
    .withColumn("payment_value", F.col("payment_value").cast("decimal(12,2)"))
    .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
    .withColumn("payment_installments", F.col("payment_installments").cast("int"))
)

(
    silver_payments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.olist_order_payments")
)

print(
    "olist_order_payments: "
    f"Silver rows={silver_payments.count()}, "
    f"Quarantined rows={invalid_payments.count()}"
)

# ---------------------------------------------------------
# 6. Automated Silver data quality gates
# ---------------------------------------------------------

def assert_zero_rows(check_name, query):
    """Fail the pipeline if a critical quality check finds rows."""
    violation_count = spark.sql(query).first()[0]

    print(f"Quality check: {check_name} = {violation_count}")

    if violation_count != 0:
        raise ValueError(
            f"DATA QUALITY FAILURE: {check_name} "
            f"found {violation_count} violating rows."
        )


# 1. Primary key uniqueness
assert_zero_rows(
    "duplicate_order_ids",
    """
    SELECT COUNT(*)
    FROM (
        SELECT order_id
        FROM de_dev.silver.olist_orders
        GROUP BY order_id
        HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_customer_ids",
    """
    SELECT COUNT(*)
    FROM (
        SELECT customer_id
        FROM de_dev.silver.olist_customers
        GROUP BY customer_id
        HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_product_ids",
    """
    SELECT COUNT(*)
    FROM (
        SELECT product_id
        FROM de_dev.silver.olist_products
        GROUP BY product_id
        HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_seller_ids",
    """
    SELECT COUNT(*)
    FROM (
        SELECT seller_id
        FROM de_dev.silver.olist_sellers
        GROUP BY seller_id
        HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_order_item_keys",
    """
    SELECT COUNT(*)
    FROM (
        SELECT order_id, order_item_id
        FROM de_dev.silver.olist_order_items
        GROUP BY order_id, order_item_id
        HAVING COUNT(*) > 1
    )
    """
)

# 2. Referential integrity
assert_zero_rows(
    "orders_without_customer",
    """
    SELECT COUNT(*)
    FROM de_dev.silver.olist_orders o
    LEFT ANTI JOIN de_dev.silver.olist_customers c
      ON o.customer_id = c.customer_id
    """
)

assert_zero_rows(
    "order_items_without_order",
    """
    SELECT COUNT(*)
    FROM de_dev.silver.olist_order_items i
    LEFT ANTI JOIN de_dev.silver.olist_orders o
      ON i.order_id = o.order_id
    """
)

assert_zero_rows(
    "order_items_without_product",
    """
    SELECT COUNT(*)
    FROM de_dev.silver.olist_order_items i
    LEFT ANTI JOIN de_dev.silver.olist_products p
      ON i.product_id = p.product_id
    """
)

assert_zero_rows(
    "order_items_without_seller",
    """
    SELECT COUNT(*)
    FROM de_dev.silver.olist_order_items i
    LEFT ANTI JOIN de_dev.silver.olist_sellers s
      ON i.seller_id = s.seller_id
    """
)


# 3. Payment key uniqueness and referential integrity
assert_zero_rows(
    "duplicate_payment_keys",
    """
    SELECT COUNT(*)
    FROM (
        SELECT order_id, payment_sequential
        FROM de_dev.silver.olist_order_payments
        GROUP BY order_id, payment_sequential
        HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "payments_without_order",
    """
    SELECT COUNT(*)
    FROM de_dev.silver.olist_order_payments p
    LEFT ANTI JOIN de_dev.silver.olist_orders o
      ON p.order_id = o.order_id
    """
)

# ---------------------------------------------------------
# Final Silver and quarantine row counts
# ---------------------------------------------------------
for table_name in [
    "olist_orders",
    "olist_order_items",
    "olist_customers",
    "olist_products",
    "olist_sellers",
    "olist_order_payments",
]:
    silver_count = spark.table(f"{SILVER}.{table_name}").count()
    quarantine_count = spark.table(f"{QUARANTINE}.{table_name}").count()
    print(
        f"{table_name}: "
        f"Silver rows={silver_count}, "
        f"Quarantined rows={quarantine_count}"
    )

print("Silver transformations and critical data quality gates completed.")

print("All critical Silver data quality gates passed.")

