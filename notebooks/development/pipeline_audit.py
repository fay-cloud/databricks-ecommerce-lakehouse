
from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
)

AUDIT_CATALOGS = {
    "dev": "de_dev",
    "test": "de_test",
    "stage": "de_stage",
    "prod": "de_prod",
}

AUDIT_SCHEMA = StructType([
    StructField("audit_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("environment", StringType(), True),
    StructField("job_run_id", StringType(), True),
    StructField("task_name", StringType(), True),
    StructField("dataset_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("status", StringType(), False),
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", LongType(), True),
    StructField("source_row_count", LongType(), True),
    StructField("output_row_count", LongType(), True),
    StructField("quarantined_row_count", LongType(), True),
    StructField("validation_status", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("created_at", TimestampType(), False),
])


def log_audit_record(
    spark,
    task_name,
    layer,
    status,
    start_time,
    dataset_name=None,
    source_row_count=None,
    output_row_count=None,
    quarantined_row_count=None,
    validation_status=None,
    error_message=None,
    job_run_id=None,
    environment="dev",
):
    """Append one task execution record to the Delta audit table."""

    end_time = datetime.now(timezone.utc)

    duration_seconds = None
    if start_time is not None:
        duration_seconds = int(
            (end_time - start_time).total_seconds()
        )

    record = [(
        str(uuid4()),
        "ecommerce_pipeline",
        environment,
        job_run_id,
        task_name,
        dataset_name,
        layer,
        status,
        start_time,
        end_time,
        duration_seconds,
        source_row_count,
        output_row_count,
        quarantined_row_count,
        validation_status,
        error_message[:4000] if error_message else None,
        end_time,
    )]

    audit_catalog = AUDIT_CATALOGS.get(environment, "de_dev")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {audit_catalog}.monitoring")
    audit_table = f"{audit_catalog}.monitoring.pipeline_audit"
    audit_df = spark.createDataFrame(record, AUDIT_SCHEMA)

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(audit_table)
    )
