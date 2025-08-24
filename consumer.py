from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StructField, StringType, LongType

# Schema 
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", LongType(), True)
])

# Initiate Spark Session
spark = SparkSession \
    .builder \
    .appName("ClickstreamProcessor") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.23") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "clickstream_events") \
    .load()

# Convert binary->json
json_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .withColumn("data", from_json(col("value"), schema)) \
    .select("data.*") \
    .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))

# Counting view in 1 min
page_view_counts = json_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("page_url")
    ) \
    .agg(count("*").alias("view_count")) \
    .select("window.start", "window.end", "page_url", "view_count")


def write_to_postgres(df, epoch_id):
    df.write \
      .format("jdbc") \
      .option("url", "jdbc:postgresql://postgres:5432/clickstream") \
      .option("dbtable", "page_views_agg") \
      .option("user", "user") \
      .option("password", "password") \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

query_postgres = page_view_counts \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("update") \
    .start()


# query_console.awaitTermination()
query_postgres.awaitTermination()