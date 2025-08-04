
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder.appName("KafkaSparkIngestion").getOrCreate()

customer_schema = StructType() \
    .add("customer_id", StringType()) \
    .add("name", StringType()) \
    .add("email", StringType()) \
    .add("phone", StringType())

customers_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "customers_topic") \
    .load()

parsed_customers = customers_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), customer_schema).alias("data")) \
    .select("data.*")

query = parsed_customers.writeStream \
    .format("console") \
    .start()

query.awaitTermination()
