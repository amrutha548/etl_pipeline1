from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType
import logging

# Logger setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaToRDS")

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreamingToRDS") \
    .config("spark.jars", "/home/ec2-user/postgresql-42.2.23.jar") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "3.108.194.241:9092"  # Replace with your Kafka server address
kafka_topic = "news_topic"

# PostgreSQL configuration
db_url = "jdbc:postgresql://newsapi-db.chus66qw2fhd.ap-south-1.rds.amazonaws.com:5432/task_pipeline"
db_properties = {
    "user": "newspostgres",
    "password": "*athira1*",  # Replace with your RDS password
    "driver": "org.postgresql.Driver"
}

# Read data from Kafka
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Define schema
schema = StructType([
    StringType().add("title", StringType()),
    StringType().add("description", StringType()),
    StringType().add("url", StringType())
])

# Parse Kafka messages
parsed_df = streaming_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*").dropna()

# Write to RDS
def write_to_rds(batch_df, batch_id):
    logger.info(f"Processing batch ID: {batch_id}")
    batch_df.write \
        .jdbc(url=db_url, table="news_articles", mode="append", properties=db_properties)

# Stream query
query = parsed_df.writeStream \
    .foreachBatch(write_to_rds) \
    .outputMode("append") \
    .start()

# Graceful termination
try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Stopping the streaming query...")
finally:
    spark.stop()

