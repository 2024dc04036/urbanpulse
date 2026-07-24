from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg, max, date_format, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("UrbanPulse-Ward-Energy-Aggregations") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint_energy") \
    .getOrCreate()

# Schema matching the smart meter payload
meter_schema = StructType([
    StructField("meter_id", StringType(), True),
    StructField("ward_id", StringType(), True),
    StructField("kwh_reading", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("power_factor", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read data from Kafka stream
kafka_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9094,localhost:9096") \
    .option("subscribe", "urbanpulse.smart_meters") \
    .load()

# Deserialize JSON contents and assign watermarks for late-data handling
meter_df = kafka_raw_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), meter_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "10 minutes")

# Execute 15-minute Tumbling Window Grouping
aggregated_df = meter_df \
    .groupBy(
        window(col("timestamp"), "15 minutes"),
        col("ward_id")
    ) \
    .agg(
        sum("kwh_reading").alias("total_kwh_consumed"),
        avg("power_factor").alias("avg_power_factor"),
        max("voltage").alias("peak_voltage")
    ) \
    .select(
        col("window.start").cast(StringType()).alias("window_start"),
        col("window.end").cast(StringType()).alias("window_end"),
        col("ward_id"),
        col("total_kwh_consumed"),
        col("avg_power_factor"),
        col("peak_voltage"),
        date_format(col("window.start"), "yyyy-MM-dd").alias("date")
    )

# --------------------------------------------------------------------------------
# CONCURRENT OUTPUT ROUTING
# --------------------------------------------------------------------------------
# Sink 1: Send summaries to Kafka for real-time alerting/dashboards
kafka_sink_query = aggregated_df \
    .selectExpr("ward_id AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9094,localhost:9096") \
    .option("topic", "ward_energy_summary") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_energy_kafka") \
    .outputMode("complete") \
    .start()

# Sink 2: Append summaries to a partitioned Parquet dataset for historical trend analysis
parquet_sink_query = aggregated_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/var/urbanpulse/analytics/ward_energy/") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_energy_parquet") \
    .partitionBy("ward_id", "date") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
