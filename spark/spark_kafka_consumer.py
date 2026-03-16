

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json
# from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType, DateType
# from pyspark.sql.functions import to_date, to_timestamp

# spark = SparkSession.builder \
#     .appName("KafkaConsumer") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# # Read Kafka stream
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "pizza_orders") \
#     .option("startingOffsets", "earliest") \
#     .load()

# json_df = df.selectExpr("CAST(value AS STRING)").writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", "false") \
#     .start() \
#     .awaitTermination()


# 

# # Define schema
# schema = StructType() \
#     .add("order_id", IntegerType()) \
#     .add("date", StringType()) \
#     .add("time", StringType())

# # Parse JSON
# parsed_df = json_df.select(
#     from_json(col("value"), schema).alias("data")
# ).select("data.*")

# parsed_df = parsed_df \
#     .withColumnRenamed("date", "order_date") \
#     .withColumnRenamed("time", "order_time")

# parsed_df = parsed_df \
#     .withColumn("order_date", to_date("order_date")) \
#     .withColumn("order_time", to_timestamp("order_time"))

# # Function to write each microbatch to PostgreSQL
# def write_to_postgres(batch_df, batch_id):
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://host.docker.internal:5432/pizza_dw") \
#         .option("dbtable", "pizza_orders_stream") \
#         .option("user", "postgres") \
#         .option("password", "rohit-21") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()

# # Stream to PostgreSQL
# query = parsed_df.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()



# 4444444444444444444444444444444444444444444444444444444444444444444444444444444444444444

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, to_date, to_timestamp
# from pyspark.sql.types import StructType, IntegerType, StringType

# spark = SparkSession.builder \
#     .appName("KafkaConsumer") \
#     .getOrCreate()

# spark.sparkContext.setLogLevel("ERROR")

# # Read Kafka stream
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "pizza_orders") \
#     .option("startingOffsets", "latest") \
#     .load()

# # Convert Kafka value to string
# # json_df = df.selectExpr("CAST(value AS STRING)")

# json_df = df.selectExpr("CAST(value AS STRING)").writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", "false") \
#     .start() \
    

# # Define schema
# schema = StructType() \
#     .add("order_id", IntegerType()) \
#     .add("date", StringType()) \
#     .add("time", StringType())

# # Parse JSON
# parsed_df = json_df.select(
#     from_json(col("value"), schema).alias("data")
# ).select("data.*")

# # Rename columns
# parsed_df = parsed_df \
#     .withColumnRenamed("date", "order_date") \
#     .withColumnRenamed("time", "order_time")

# # Convert types
# parsed_df = parsed_df \
#     .withColumn("order_date", to_date("order_date")) \
#     .withColumn("order_time", to_timestamp("order_time"))

# # Function to write batch to PostgreSQL
# def write_to_postgres(batch_df, batch_id):
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://172.19.144.1:5432/pizza_dw") \
#         .option("dbtable", "pizza_orders_stream") \
#         .option("user", "postgres") \
#         .option("password", "rohit-21") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()

# # Start streaming
# query = parsed_df.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .option("checkpointLocation", "/tmp/pizza_orders_checkpoint") \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()



# 444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import StructType, IntegerType, StringType
import psycopg2

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pizza_orders") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as value")

schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("date", StringType()) \
    .add("time", StringType())

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df \
    .withColumnRenamed("date", "order_date") \
    .withColumnRenamed("time", "order_time")

parsed_df = parsed_df \
    .withColumn("order_date", to_date("order_date")) \
    .withColumn("order_time", to_timestamp("order_time"))

# def write_to_postgres(batch_df, batch_id):
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://172.19.144.1:5432/pizza_dw") \
#         .option("dbtable", "pizza_orders_stream") \
#         .option("user", "postgres") \
#         .option("password", "rohit-21") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()




def write_to_postgres(batch_df, batch_id):

    # remove duplicates in the same microbatch
    batch_df = batch_df.dropDuplicates(["order_id"])

    # write to staging table
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.19.144.1:5432/pizza_dw") \
        .option("dbtable", "pizza_orders_stage") \
        .option("user", "postgres") \
        .option("password", "rohit-21") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    import psycopg2

    conn = psycopg2.connect(
        host="172.19.144.1",
        database="pizza_dw",
        user="postgres",
        password="rohit-21"
    )

    cursor = conn.cursor()

    # UPSERT from staging → final table
    cursor.execute("""
        INSERT INTO pizza_orders_stream
        SELECT DISTINCT ON (order_id)
            order_id,
            order_date,
            order_time
        FROM pizza_orders_stage
        ORDER BY order_id
        ON CONFLICT (order_id)
        DO UPDATE SET
            order_date = EXCLUDED.order_date,
            order_time = EXCLUDED.order_time;
    """)

    # clear staging table
    cursor.execute("TRUNCATE pizza_orders_stage")

    conn.commit()
    cursor.close()
    conn.close()



query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/pizza_orders_checkpoint") \
    .start()

query.awaitTermination()