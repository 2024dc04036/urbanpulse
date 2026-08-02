from pyspark.sql import SparkSession
from pyspark.sql import functions as F
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
    StructField("timestamp", StringType(), True)
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
    .select(F.from_json(F.col("json_payload"), meter_schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withWatermark("timestamp", "10 minutes")
 
# Execute 15-minute Tumbling Window Grouping
aggregated_df = meter_df \
    .groupBy(
        F.window(F.col("timestamp"), "15 minutes"),
        F.col("ward_id")
    ) \
    .agg(
        F.sum("kwh_reading").alias("total_kwh_consumed"),
        F.avg("power_factor").alias("avg_power_factor"),
        F.max("voltage").alias("peak_voltage")
    ) \
    .select(
        F.col("window.start").cast(StringType()).alias("window_start"),
        F.col("window.end").cast(StringType()).alias("window_end"),
        F.col("ward_id"),
        F.col("total_kwh_consumed"),
        F.col("avg_power_factor"),
        F.col("peak_voltage"),
        F.date_format(F.col("window.start"), "yyyy-MM-dd").alias("date")
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
    .outputMode("update") \
    .start()
 
# Sink 2: Append summaries to a partitioned Parquet dataset for historical trend analysis
parquet_sink_query = aggregated_df \
    .writeStream \
    .format("parquet") \
    .option("path", "/var/urbanpulse/analytics/ward_energy/") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_energy_parquet") \
    .partitionBy("date", "ward_id") \
    .outputMode("append") \
    .start()
 
spark.streams.awaitAnyTermination()
 
