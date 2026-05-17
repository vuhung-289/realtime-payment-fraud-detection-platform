from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PAYMENT_EVENT_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), False),
        StructField("event_time", TimestampType(), True),
        StructField("user_id", StringType(), False),
        StructField("device_id", StringType(), True),
        StructField("country", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("is_international", StringType(), True),
    ]
)

