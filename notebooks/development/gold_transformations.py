# GOLD LAYER LOGIC
# - Dimensional modelling (facts and dimensions)
# - Business-key and surrogate-key management
# - Fact and dimension table creation
# - Aggregations and business metric calculations
# - Revenue, sales, seller, product, and delivery analytics
# - Customer retention and lifetime value metrics
# - Data reconciliation against Silver
# - Gold-level data quality and uniqueness checks
# - Incremental MERGE and CDC propagation (planned)
# - Historical tracking and SCD-aware reporting (planned)
# - BI and dashboard-ready data products 
 
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pipeline_audit import log_audit_record

PIPELINE_START = datetime.now(timezone.utc)
AUDIT_TASK_NAME = "gold_transformations"
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", default="de_dev")
parser.add_argument("--environment", default="dev")
args, _ = parser.parse_known_args()

CATALOG = args.catalog
ENVIRONMENT = args.environment


def write_gold_audit(**kwargs):
    """Write a Gold audit record without masking transformation results."""
    try:
        log_audit_record(
            spark=spark,
            task_name=AUDIT_TASK_NAME,
            environment=ENVIRONMENT,
            **kwargs,
        )
    except Exception as audit_error:
        print(
            "WARNING: Gold audit write failed: "
            f"{type(audit_error).__name__}: {audit_error}"
        )

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD}")

# ---------------------------------------------------------
# 1. Dimensions
# ---------------------------------------------------------
customers = spark.table(f"{SILVER}.olist_customers")
products = spark.table(f"{SILVER}.olist_products")
sellers = spark.table(f"{SILVER}.olist_sellers")
translations = spark.table(f"{SILVER}.product_category_name_translation")

# Customer dimension: one row per customer_id.
dim_customer = customers.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
).dropDuplicates(["customer_id"])

# Product dimension: one row per product_id, with English category where available.
dim_product = (
    products.alias("p")
    .join(
        translations.select(
            "product_category_name",
            "product_category_name_english"
        ).dropDuplicates(["product_category_name"]).alias("t"),
        F.col("p.product_category_name") == F.col("t.product_category_name"),
        "left"
    )
    .select(
        F.col("p.product_id"),
        F.col("p.product_category_name"),
        F.col("t.product_category_name_english"),
        *[
            F.col(f"p.{c}")
            for c in [
                "product_name_lenght",
                "product_description_lenght",
                "product_photos_qty",
                "product_weight_g",
                "product_length_cm",
                "product_height_cm",
                "product_width_cm",
            ]
            if c in products.columns
        ],
    )
    .dropDuplicates(["product_id"])
)

dim_seller = sellers.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
).dropDuplicates(["seller_id"])

for table_name, dataframe in [
    ("dim_customer", dim_customer),
    ("dim_product", dim_product),
    ("dim_seller", dim_seller),
]:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.{table_name}")
    )
    print(f"{table_name}: rows={dataframe.count()}")

# ---------------------------------------------------------
# 2. Fact order items
# Grain: one row per order_id + order_item_id.
# ---------------------------------------------------------
orders = spark.table(f"{SILVER}.olist_orders")
items = spark.table(f"{SILVER}.olist_order_items")

order_columns = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
available_order_columns = [c for c in order_columns if c in orders.columns]

fact_order_items = (
    items.alias("i")
    .join(
        orders.select(*available_order_columns).dropDuplicates(["order_id"]).alias("o"),
        on="order_id",
        how="left"
    )
    .withColumn(
        "order_purchase_date",
        F.to_date(F.col("order_purchase_timestamp"))
    )
    .withColumn(
        "order_purchase_date_key",
        F.date_format(F.col("order_purchase_timestamp"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "delivery_days",
        F.datediff(
            F.to_date(F.col("order_delivered_customer_date")),
            F.to_date(F.col("order_purchase_timestamp"))
        )
    )
    .withColumn(
        "estimated_delivery_days",
        F.datediff(
            F.to_date(F.col("order_estimated_delivery_date")),
            F.to_date(F.col("order_purchase_timestamp"))
        )
    )
    .withColumn(
        "is_delivered_late",
        F.when(
            F.col("order_delivered_customer_date").isNull()
            | F.col("order_estimated_delivery_date").isNull(),
            F.lit(None).cast("boolean")
        ).otherwise(
            F.to_date("order_delivered_customer_date")
            > F.to_date("order_estimated_delivery_date")
        )
    )
    .withColumn(
        "item_total",
        (F.col("price") + F.col("freight_value")).cast("decimal(14,2)")
    )
    .select(
        "order_id",
        "order_item_id",
        "customer_id",
        "product_id",
        "seller_id",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date",
        "order_purchase_date_key",
        "order_approved_at",
        "shipping_limit_date",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "delivery_days",
        "estimated_delivery_days",
        "is_delivered_late",
        "price",
        "freight_value",
        "item_total",
    )
)

(
    fact_order_items.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.fact_order_items")
)
print(f"fact_order_items: rows={fact_order_items.count()}")

# ---------------------------------------------------------
# 3. Fact order payments
# Grain: one row per order_id + payment_sequential.
# Payments remain separate from item facts to avoid fan-out/double counting.
# ---------------------------------------------------------
payments = spark.table(f"{SILVER}.olist_order_payments")

fact_order_payments = (
    payments.alias("p")
    .join(
        orders.select("order_id", "customer_id", "order_status", "order_purchase_timestamp")
        .dropDuplicates(["order_id"]).alias("o"),
        on="order_id",
        how="left"
    )
    .withColumn(
        "order_purchase_date_key",
        F.date_format(F.col("order_purchase_timestamp"), "yyyyMMdd").cast("int")
    )
    .select(
        "order_id",
        "payment_sequential",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_purchase_date_key",
        "payment_type",
        "payment_installments",
        "payment_value",
    )
)

(
    fact_order_payments.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.fact_order_payments")
)
print(f"fact_order_payments: rows={fact_order_payments.count()}")

# ---------------------------------------------------------
# 4. Fact order reviews
# Grain: one source review record. Composite hash preserves rows even when
# review_id repeats across different orders; duplicate IDs remain observable.
# ---------------------------------------------------------
reviews = spark.table(f"{SILVER}.olist_order_reviews")

review_window = Window.partitionBy("review_id")
fact_order_reviews = (
    reviews
    .withColumn(
        "review_row_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("review_id"), F.lit("")),
                F.coalesce(F.col("order_id"), F.lit("")),
                F.coalesce(F.col("review_score").cast("string"), F.lit("")),
                F.coalesce(F.col("review_creation_date").cast("string"), F.lit("")),
                F.coalesce(F.col("review_answer_timestamp").cast("string"), F.lit("")),
                F.coalesce(F.col("review_comment_title"), F.lit("")),
                F.coalesce(F.col("review_comment_message"), F.lit("")),
            ),
            256
        )
    )
    .withColumn("review_id_occurrences", F.count(F.lit(1)).over(review_window))
    .withColumn("is_duplicate_review_id", F.col("review_id_occurrences") > 1)
    .select(
        "review_row_key",
        "review_id",
        "order_id",
        "review_score",
        "review_comment_title",
        "review_comment_message",
        "review_creation_date",
        "review_answer_timestamp",
        "review_id_occurrences",
        "is_duplicate_review_id",
    )
)

(
    fact_order_reviews.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.fact_order_reviews")
)
print(f"fact_order_reviews: rows={fact_order_reviews.count()}")

# ---------------------------------------------------------
# 5. Daily sales aggregate
# Grain: one row per purchase date and order status.
# Uses item price and freight only; payment values are not joined here.
# ---------------------------------------------------------
gold_daily_sales = (
    fact_order_items
    .filter(F.col("order_purchase_date").isNotNull())
    .groupBy("order_purchase_date", "order_status")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.count("*").alias("order_item_count"),
        F.sum("price").cast("decimal(18,2)").alias("product_revenue"),
        F.sum("freight_value").cast("decimal(18,2)").alias("freight_revenue"),
        F.sum("item_total").cast("decimal(18,2)").alias("item_plus_freight_revenue"),
        F.avg("price").cast("decimal(18,2)").alias("average_item_price"),
        F.avg(F.col("is_delivered_late").cast("int")).alias("late_delivery_rate"),
    )
    .withColumn(
        "order_purchase_date_key",
        F.date_format("order_purchase_date", "yyyyMMdd").cast("int")
    )
    .select(
        "order_purchase_date_key",
        "order_purchase_date",
        "order_status",
        "order_count",
        "order_item_count",
        "product_revenue",
        "freight_revenue",
        "item_plus_freight_revenue",
        "average_item_price",
        "late_delivery_rate",
    )
)

(
    gold_daily_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD}.gold_daily_sales")
)
print(f"gold_daily_sales: rows={gold_daily_sales.count()}")

# ---------------------------------------------------------
# 6. Gold quality gates
# ---------------------------------------------------------
def assert_zero_rows(check_name, query):
    violations = spark.sql(query).first()[0]
    print(f"Gold quality check: {check_name} = {violations}")
    if violations != 0:
        raise ValueError(
            f"GOLD DATA QUALITY FAILURE: {check_name} found {violations} violations."
        )

assert_zero_rows(
    "duplicate_dim_customer_keys",
    f"""
    SELECT COUNT(*) FROM (
        SELECT customer_id FROM {GOLD}.dim_customer
        GROUP BY customer_id HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_dim_product_keys",
    f"""
    SELECT COUNT(*) FROM (
        SELECT product_id FROM {GOLD}.dim_product
        GROUP BY product_id HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_dim_seller_keys",
    f"""
    SELECT COUNT(*) FROM (
        SELECT seller_id FROM {GOLD}.dim_seller
        GROUP BY seller_id HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_fact_order_item_keys",
    f"""
    SELECT COUNT(*) FROM (
        SELECT order_id, order_item_id FROM {GOLD}.fact_order_items
        GROUP BY order_id, order_item_id HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_fact_payment_keys",
    f"""
    SELECT COUNT(*) FROM (
        SELECT order_id, payment_sequential FROM {GOLD}.fact_order_payments
        GROUP BY order_id, payment_sequential HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "duplicate_fact_review_row_keys",
    f"""
    SELECT COUNT(*) FROM (
        SELECT review_row_key FROM {GOLD}.fact_order_reviews
        GROUP BY review_row_key HAVING COUNT(*) > 1
    )
    """
)

assert_zero_rows(
    "fact_order_items_reconciliation",
    f"""
    SELECT ABS(
        (SELECT COUNT(*) FROM {GOLD}.fact_order_items) -
        (SELECT COUNT(*) FROM {SILVER}.olist_order_items)
    )
    """
)

assert_zero_rows(
    "fact_order_payments_reconciliation",
    f"""
    SELECT ABS(
        (SELECT COUNT(*) FROM {GOLD}.fact_order_payments) -
        (SELECT COUNT(*) FROM {SILVER}.olist_order_payments)
    )
    """
)

assert_zero_rows(
    "fact_order_reviews_reconciliation",
    f"""
    SELECT ABS(
        (SELECT COUNT(*) FROM {GOLD}.fact_order_reviews) -
        (SELECT COUNT(*) FROM {SILVER}.olist_order_reviews)
    )
    """
)

# ---------------------------------------------------------
# 7. Gold audit records
# Audit records are written after all critical quality gates pass.
# ---------------------------------------------------------
GOLD_AUDIT_DATASETS = [
    # dataset_name, source_table, output_table
    ("dim_customer", f"{SILVER}.olist_customers", f"{GOLD}.dim_customer"),
    ("dim_product", f"{SILVER}.olist_products", f"{GOLD}.dim_product"),
    ("dim_seller", f"{SILVER}.olist_sellers", f"{GOLD}.dim_seller"),
    ("fact_order_items", f"{SILVER}.olist_order_items", f"{GOLD}.fact_order_items"),
    ("fact_order_payments", f"{SILVER}.olist_order_payments", f"{GOLD}.fact_order_payments"),
    ("fact_order_reviews", f"{SILVER}.olist_order_reviews", f"{GOLD}.fact_order_reviews"),
    # Daily sales aggregates the order-item fact. Source count is the full
    # input fact row count; output count is the number of aggregate rows.
    ("gold_daily_sales", f"{GOLD}.fact_order_items", f"{GOLD}.gold_daily_sales"),
]

for dataset_name, source_table, output_table in GOLD_AUDIT_DATASETS:
    source_count = spark.table(source_table).count()
    output_count = spark.table(output_table).count()

    print(
        f"Gold audit - {dataset_name}: "
        f"source_rows={source_count}, output_rows={output_count}"
    )

    write_gold_audit(
        dataset_name=dataset_name,
        layer="gold",
        status="SUCCESS",
        start_time=PIPELINE_START,
        source_row_count=source_count,
        output_row_count=output_count,
        quarantined_row_count=0,
        validation_status="PASSED",
    )

write_gold_audit(
    dataset_name=None,
    layer="gold",
    status="SUCCESS",
    start_time=PIPELINE_START,
    validation_status="PASSED",
)

print("Gold transformations and critical quality gates completed.")
