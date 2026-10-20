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
# Reviews: validate identifiers and review scores
# ---------------------------------------------------------
reviews = spark.table(f"{BRONZE}.olist_order_reviews")

reviews_checked = (
    reviews
    .withColumn(
        "_review_score_int",
        F.expr("try_cast(review_score AS INT)")
    )
    .withColumn(
        "_rejection_reason",
        F.when(
            F.col("review_id").isNull()
            | (~F.col("review_id").rlike("^[0-9a-fA-F]{32}$")),
            "invalid_review_id"
        )
        .when(
            F.col("order_id").isNull()
            | (~F.col("order_id").rlike("^[0-9a-fA-F]{32}$")),
            "invalid_order_id"
        )
        .when(
            F.col("_review_score_int").isNull(),
            "missing_or_invalid_review_score"
        )
        .when(
            (F.col("_review_score_int") < 1)
            | (F.col("_review_score_int") > 5),
            "review_score_out_of_range"
        )
    )
)

invalid_reviews = reviews_checked.filter(
    F.col("_rejection_reason").isNotNull()
)

valid_reviews = reviews_checked.filter(
    F.col("_rejection_reason").isNull()
)

(
    invalid_reviews
    .select(*reviews.columns, "_rejection_reason")
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{QUARANTINE}.olist_order_reviews")
)

silver_reviews = (
    valid_reviews
    .drop("_rejection_reason", "_rescued_data", "review_score")
    .withColumnRenamed("_review_score_int", "review_score")
    .withColumn("review_id", F.lower(F.trim("review_id")))
    .withColumn("order_id", F.lower(F.trim("order_id")))
    .withColumn("review_comment_title", F.trim("review_comment_title"))
    .withColumn("review_comment_message", F.trim("review_comment_message"))
)

(
    silver_reviews
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.olist_order_reviews")
)

print(
    "olist_order_reviews: "
    f"Silver rows={silver_reviews.count()}, "
    f"Quarantined rows={invalid_reviews.count()}"
)

# ---------------------------------------------------------
# 8. Geolocation: validate coordinates and standardise labels
# ---------------------------------------------------------
geolocation = spark.table(f"{BRONZE}.olist_geolocation")

# Parse coordinates safely so malformed strings are quarantined, not fatal.
geolocation_checked = (
    geolocation
    .withColumn("_latitude_decimal", F.expr("try_cast(geolocation_lat AS DECIMAL(10,7))"))
    .withColumn("_longitude_decimal", F.expr("try_cast(geolocation_lng AS DECIMAL(10,7))"))
    .withColumn(
        "_rejection_reason",
        F.when(F.col("geolocation_zip_code_prefix").isNull(), "missing_zip_code_prefix")
         .when(F.col("_latitude_decimal").isNull(), "missing_or_invalid_latitude")
         .when(F.col("_longitude_decimal").isNull(), "missing_or_invalid_longitude")
         .when((F.col("_latitude_decimal") < -90) | (F.col("_latitude_decimal") > 90),
               "latitude_out_of_range")
         .when((F.col("_longitude_decimal") < -180) | (F.col("_longitude_decimal") > 180),
               "longitude_out_of_range")
    )
)

invalid_geolocation = geolocation_checked.filter(
    F.col("_rejection_reason").isNotNull()
)
valid_geolocation = geolocation_checked.filter(
    F.col("_rejection_reason").isNull()
)

(
    invalid_geolocation.select(*geolocation.columns, "_rejection_reason")
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{QUARANTINE}.olist_geolocation")
)

silver_geolocation = (
    valid_geolocation
    .drop(
        "_rejection_reason", "_rescued_data",
        "geolocation_lat", "geolocation_lng"
    )
    .withColumnRenamed("_latitude_decimal", "geolocation_lat")
    .withColumnRenamed("_longitude_decimal", "geolocation_lng")
    .withColumn(
        "geolocation_zip_code_prefix",
        F.lpad(F.col("geolocation_zip_code_prefix").cast("string"), 5, "0")
    )
    .withColumn("geolocation_city", F.lower(F.trim("geolocation_city")))
    .withColumn("geolocation_state", F.upper(F.trim("geolocation_state")))
)

(
    silver_geolocation.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{SILVER}.olist_geolocation")
)

# Geolocation intentionally has no uniqueness gate: many rows can share a ZIP prefix.

# ---------------------------------------------------------
# 9. Product category translation: validate and standardise
# ---------------------------------------------------------
translations = spark.table(
    f"{BRONZE}.product_category_translation"
)

translations_checked = translations.withColumn(
    "_rejection_reason",
    F.when(
        F.col("product_category_name").isNull(),
        "missing_product_category_name"
    )
    .when(
        F.trim(F.col("product_category_name")) == "",
        "blank_product_category_name"
    )
    .when(
        F.col("product_category_name_english").isNull(),
        "missing_english_category_name"
    )
    .when(
        F.trim(F.col("product_category_name_english")) == "",
        "blank_english_category_name"
    )
)

invalid_translations = translations_checked.filter(
    F.col("_rejection_reason").isNotNull()
)

valid_translations = translations_checked.filter(
    F.col("_rejection_reason").isNull()
)

# Quarantine invalid category translations.
(
    invalid_translations
    .select(*translations.columns, "_rejection_reason")
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{QUARANTINE}.product_category_name_translation"
    )
)

# Standardise valid category translations.
silver_translations = (
    valid_translations
    .drop("_rejection_reason", "_rescued_data")
    .withColumn(
        "product_category_name",
        F.lower(F.trim("product_category_name"))
    )
    .withColumn(
        "product_category_name_english",
        F.lower(F.trim("product_category_name_english"))
    )
)

# Write the Silver table.
(
    silver_translations
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        f"{SILVER}.product_category_name_translation"
    )
)

print(
    "product_category_name_translation: "
    f"Silver rows={silver_translations.count()}, "
    f"Quarantined rows={invalid_translations.count()}"
)


# ---------------------------------------------------------
# 10. Automated Silver data quality gates
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


# 4. Reviews, geolocation, and category translation quality gates

# Review IDs may repeat across different orders in the current source.
# Monitor duplicate IDs without failing the entire pipeline.
duplicate_review_count = spark.sql("""
    SELECT COUNT(*)
    FROM (
        SELECT review_id
        FROM de_dev.silver.olist_order_reviews
        GROUP BY review_id
        HAVING COUNT(*) > 1
    )
""").first()[0]

print(f"Quality check: duplicate_review_ids = {duplicate_review_count}")



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
    "olist_order_reviews",
    "olist_geolocation",
    "product_category_name_translation",
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

