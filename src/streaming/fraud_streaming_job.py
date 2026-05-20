import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import json
import os

os.environ["HADOOP_HOME"] = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "hadoop")
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

import pandas as pd
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from src.common.risk_rules import compute_risk_score
from src.common.settings import Settings


def write_to_bigquery(pdf: pd.DataFrame, table_id: str, project: str) -> None:
    if pdf.empty:
        return
    client = bigquery.Client(project=project, location="asia-southeast1")  # ← thêm location
    
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",  # ← tự tạo table nếu chưa có
    )
    
    job = client.load_table_from_dataframe(pdf, table_id, job_config=job_config)
    job.result()


def main() -> None:
    settings = Settings()
    spark = (
        SparkSession.builder.appName("fraud_streaming_job")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic_payments)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    event_schema = StructType(
        [
            StructField("transaction_id", StringType(), False),
            StructField("event_time", StringType(), True),
            StructField("user_id", StringType(), False),
            StructField("device_id", StringType(), True),
            StructField("country", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("amount", StringType(), True),
            StructField("currency", StringType(), True),
            StructField("payment_status", StringType(), True),
            StructField("is_international", StringType(), True),
        ]
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), event_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("is_international", (F.col("is_international") == F.lit("true")))
        .filter(F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull() & F.col("event_time").isNotNull())
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["transaction_id"])
    )

    scored_table = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.fraud_scored_transactions"
    alerts_table = f"{settings.gcp_project_id}.{settings.bigquery_dataset}.fraud_alerts"

    def process_batch(batch_df, batch_id: int) -> None:
        pdf = batch_df.toPandas()
        if pdf.empty:
            return

        user_stats = (
            pdf.groupby("user_id")
            .agg(
                avg_amount_30m=("amount", "mean"),
                tx_count_5m=("transaction_id", "count"),
                failed_count_5m=("payment_status", lambda s: int((s == "failed").sum())),
            )
            .reset_index()
        )
        scored_pdf = pdf.merge(user_stats, on="user_id", how="left")
        scored_pdf["is_new_country"] = scored_pdf["country"] != "VN"
        scored_pdf["is_new_device"] = scored_pdf["device_id"].fillna("").str.endswith("99")

        scores = scored_pdf.apply(
            lambda row: compute_risk_score(
                amount=float(row["amount"] or 0),
                avg_amount_30m=float(row["avg_amount_30m"] or 0),
                tx_count_5m=int(row["tx_count_5m"] or 0),
                failed_count_5m=int(row["failed_count_5m"] or 0),
                is_new_country=bool(row["is_new_country"]),
                is_new_device=bool(row["is_new_device"]),
                is_international=bool(row["is_international"]),
            ),
            axis=1,
        )
        scored_pdf["risk_score"] = [x[0] for x in scores]
        scored_pdf["risk_band"] = [x[1] for x in scores]
        scored_pdf["risk_reasons"] = [json.dumps(x[2]) for x in scores]

        output_cols = [
            "transaction_id",
            "event_time",
            "user_id",
            "device_id",
            "country",
            "merchant_id",
            "amount",
            "currency",
            "payment_status",
            "is_international",
            "risk_score",
            "risk_band",
            "risk_reasons",
        ]
        scored_output = scored_pdf[output_cols].copy()
        alert_output = scored_output[scored_output["risk_score"] >= settings.alert_risk_threshold].copy()

        write_to_bigquery(scored_output, scored_table, settings.gcp_project_id)
        write_to_bigquery(alert_output, alerts_table, settings.gcp_project_id)
        print(
            f"batch={batch_id} wrote_scored={len(scored_output)} "
            f"wrote_alerts={len(alert_output)}"
        )

    query = (
        parsed.writeStream.outputMode("append")
        .foreachBatch(process_batch)
        .option("checkpointLocation", "checkpoints/scored")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

