import os
from typing import Literal

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, window
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert

from system.schemas import SENSOR_SCHEMA

# PostgreSQL connection string (Ideally fetched from environment variables)
DB_URI = os.getenv(
    "POSTGRES_URI", "postgresql+psycopg2://user:password@localhost:5432/your_db"
)


def postgres_upsert(table, conn, keys, data_iter):
    """
    Custom insertion method for Pandas to execute a PostgreSQL UPSERT.
    """
    # Convert data iterator into a list of dictionaries
    data = [dict(zip(keys, row)) for row in data_iter]
    if not data:
        return

    # Build the PostgreSQL-specific insert statement
    insert_stmt = pg_insert(table.table).values(data)

    # Define the upsert behavior (ON CONFLICT DO UPDATE)
    # Target the primary key columns
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["site_id", "pollutant_type", "start_time"],
        set_={
            # Dynamically update all columns EXCEPT the primary keys
            c.name: c
            for c in insert_stmt.excluded
            if c.name not in ["site_id", "pollutant_type", "start_time"]
        },
    )

    # Execute the statement
    conn.execute(upsert_stmt)


def write_to_postgres(df_batch, batch_id):
    """
    ForeachBatch function to write micro-batches to Postgres via SQLAlchemy.
    """
    pdf = df_batch.toPandas()

    if pdf.empty:
        return

    engine = create_engine(DB_URI)
    with engine.begin() as conn:
        pdf.to_sql(
            name="sensor_averages_hourly",
            con=conn,
            if_exists="append",
            index=False,
            method=postgres_upsert,  # Use our custom upsert function here
        )


def run_dataflow(
    appname: str,
    kafka_bootstrap_servers: str,
    input_topic: str,
    output_mode: Literal["append", "complete", "update"] = "update",
    checkpoint_location: os.PathLike | str = "/tmp/checkpoints",
) -> bool:

    spark = SparkSession.builder.appName(appname).getOrCreate()

    # Ingestion and processing from kafka topic
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), SENSOR_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("timestamp").cast("timestamp"))
        # Ensure watermarking to handle late data in streaming aggregations
        .withWatermark("event_time", "2 hours")
    )

    # Aggregation logic: 1-hour tumbling windows
    hourly_averages = (
        parsed.groupBy(
            window(col("event_time"), "1 hour").alias("time_window"),
            col("site_id"),
            col("pollutant_type"),
        )
        .agg(avg("concentration").alias("avg_value"))
        # Extract the start time from the window struct to match the Postgres schema
        .select(
            col("site_id"),
            col("pollutant_type"),
            col("time_window.start").alias("start_time"),
            col("avg_value"),
        )
    )

    # Write stream to PostgreSQL using foreachBatch
    query = (
        hourly_averages.writeStream.outputMode(output_mode)
        .foreachBatch(write_to_postgres)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )

    query.awaitTermination()
    return True
