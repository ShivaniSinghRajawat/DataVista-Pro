from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType
from pymongo import MongoClient

# Mongo insert function
def write_to_mongodb(batch_df, epoch_id):
    records = batch_df.toJSON().map(lambda j: eval(j)).collect()
    if records:
        client = MongoClient("mongodb://localhost:28017/")
        db = client["order_db"]
        collection = db["products"]
        collection.insert_many(records)

# Define schema
schema = StructType() \
    .add("id", StringType()) \
    .add("category", StringType()) \
    .add("created_at", StringType()) \
    .add("ean", StringType()) \
    .add("price", FloatType()) \
    .add("rating", FloatType()) \
    .add("title", StringType()) \
    .add("vendor", StringType())

# Spark session
spark = SparkSession.builder \
    .appName("KafkaToMongoDB") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "order-events") \
    .load()

# Parse JSON
value_df = df.selectExpr("CAST(value AS STRING)")
json_df = value_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Stream to MongoDB
query = json_df.writeStream \
    .foreachBatch(write_to_mongodb) \
    .outputMode("append") \
    .start()

query.awaitTermination()
