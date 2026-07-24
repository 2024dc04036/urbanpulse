from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

spark = SparkSession.builder \
    .appName("UrbanPulse-Health-Advisory-SQL") \
    .getOrCreate()

aqi_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read incoming air quality stream from Kafka
aqi_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "urbanpulse.air_quality") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), aqi_schema).alias("parsed")) \
    .select("parsed.*") \
    .withWatermark("timestamp", "5 minutes")

# Register streaming view
aqi_stream_df.createOrReplaceTempView("streaming_aqi")

# Hydrate the static zone profiles from disk storage (contains population, schools, etc.)
zone_profile_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/var/urbanpulse/metadata/zone_profile.csv")

zone_profile_df.createOrReplaceTempView("static_zone_profile")

# --------------------------------------------------------------------------------
# STREAMING SQL PROCESSING TOPOLOGY
# --------------------------------------------------------------------------------
health_advisory_summary = spark.sql("""
    SELECT 
        CAST(window.end AS STRING) as alert_time,
        stream.zone,
        AVG(stream.aqi) as rolling_avg_aqi,
        profile.zone_name,
        profile.population,
        profile.number_schools,
        CASE 
            WHEN AVG(stream.aqi) > 200 THEN 'CRITICAL: Enact immediate indoor schooling protocol'
            ELSE 'WARNING: Restrict outdoor physical activity for vulnerable age groups'
        END as health_advisory_text
    FROM streaming_aqi stream
    INNER JOIN static_zone_profile profile 
        ON stream.zone = profile.zone_id
    GROUP BY 
        window(stream.timestamp, "10 minutes", "5 minutes"),
        stream.zone,
        profile.zone_name,
        profile.population,
        profile.number_schools
    HAVING rolling_avg_aqi > 150
""")

# Route advisories to Kafka using Update mode to emit updated windows efficiently
advisory_query = health_advisory_summary \
    .selectExpr("zone AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "urbanpulse.health_advisories") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_health_sql") \
    .outputMode("update") \
    .start()

advisory_query.awaitTermination()
