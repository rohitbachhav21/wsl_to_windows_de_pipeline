

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType, DateType

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pizza_orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Convert Kafka value to string
json_df = df.selectExpr("CAST(value AS STRING)")

# Define schema
schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("order_date", DateType()) \
    .add("order_time", TimestampType())

# Parse JSON
parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Function to write each microbatch to PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host.docker.internal:5432/pizza_dw") \
        .option("dbtable", "pizza_orders_stream") \
        .option("user", "postgres") \
        .option("password", "rohit-21") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

# Stream to PostgreSQL
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# spark = SparkSession.builder \
#     .appName("KafkaConsumer") \
#     .getOrCreate()

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "pizza_orders") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Convert Kafka value to string
# value_df = df.selectExpr("CAST(value AS STRING) as order_json")

# def write_to_postgres(batch_df, batch_id):

#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://localhost:5432/pizza_dw") \
#         .option("dbtable", "pizza_orders_stream") \
#         .option("user", "postgres") \
#         .option("password", "rohit-21") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()

# query = value_df.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()
