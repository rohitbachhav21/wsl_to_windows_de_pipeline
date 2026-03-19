from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import StructType, IntegerType, StringType
import psycopg2

spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------
# ORDERS STREAM
# -------------------------------

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pizza_orders") \
    .option("startingOffsets", "earliest") \
    .load()

pizza_orders_json_df = df.selectExpr("CAST(value AS STRING) as value")

# -------------------------------
# ORDER DETAILS STREAM
# -------------------------------

order_details_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "pizza_order_details") \
    .option("startingOffsets", "earliest") \
    .load()

order_details_json_df = order_details_df.selectExpr("CAST(value AS STRING) as value")

# -------------------------------
# STATIC TABLES FROM MYSQL
# -------------------------------

pizzas_df = spark.read \
    .format("jdbc") \
    .option("url","jdbc:mysql://172.19.144.1:3306/de_pipeline") \
    .option("dbtable","pizzas") \
    .option("user","spark_user") \
    .option("password","rohit-21") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .load()


pizza_types_df = spark.read \
    .format("jdbc") \
    .option("url","jdbc:mysql://172.19.144.1:3306/de_pipeline") \
    .option("dbtable","pizza_types") \
    .option("user","spark_user") \
    .option("password","rohit-21") \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .load()

# -------------------------------
# SCHEMAS
# -------------------------------

schema = StructType() \
    .add("order_id", IntegerType()) \
    .add("date", StringType()) \
    .add("time", StringType())

order_details_schema = StructType() \
    .add("order_details_id", IntegerType()) \
    .add("order_id", IntegerType()) \
    .add("pizza_id", StringType()) \
    .add("quantity", IntegerType())

# -------------------------------
# PARSE ORDERS
# -------------------------------

parsed_df = pizza_orders_json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

parsed_df = parsed_df \
    .withColumnRenamed("date", "order_date") \
    .withColumnRenamed("time", "order_time")

parsed_df = parsed_df \
    .withColumn("order_date", to_date("order_date")) \
    .withColumn("order_time", to_timestamp("order_time"))

# -------------------------------
# PARSE ORDER DETAILS
# -------------------------------

parsed_order_details = order_details_json_df.select(
    from_json(col("value"), order_details_schema).alias("data")
).select("data.*")

# -------------------------------
# STREAM JOINS
# -------------------------------

orders_with_details = parsed_df.join(
    parsed_order_details,
    "order_id"
)

orders_pizza_join = orders_with_details.join(
    pizzas_df,
    "pizza_id"
)

final_stream_df = orders_pizza_join.join(
    pizza_types_df,
    "pizza_type_id"
)

# -------------------------------
# ANALYTICS COLUMN
# -------------------------------

analytics_df = final_stream_df.withColumn(
    "revenue",
    col("quantity") * col("price")
).dropDuplicates(["order_details_id"])

dataframe = analytics_df.printSchema()
print(dataframe)

# -------------------------------
# WRITE ANALYTICS TO POSTGRES
# -------------------------------

def write_analytics_to_postgres(batch_df, batch_id):

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.19.144.1:5432/pizza_dw") \
        .option("dbtable", "pizza_sales_stream") \
        .option("user", "postgres") \
        .option("password", "rohit-21") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

analytics_query = analytics_df.writeStream \
    .foreachBatch(write_analytics_to_postgres) \
    .option("checkpointLocation", "/tmp/pizza_sales_checkpoint") \
    .start()

# -------------------------------
# ORDERS UPSERT PIPELINE
# -------------------------------

def write_to_postgres(batch_df, batch_id):

    batch_df = batch_df.dropDuplicates(["order_id"])

    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://172.19.144.1:5432/pizza_dw") \
        .option("dbtable", "pizza_orders_stage") \
        .option("user", "postgres") \
        .option("password", "rohit-21") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    conn = psycopg2.connect(
        host="172.19.144.1",
        database="pizza_dw",
        user="postgres",
        password="rohit-21"
    )

    cursor = conn.cursor()

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

    cursor.execute("TRUNCATE pizza_orders_stage")

    conn.commit()
    cursor.close()
    conn.close()

query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/pizza_orders_checkpoint") \
    .start()

# -------------------------------
# RUN STREAMS
# -------------------------------

spark.streams.awaitAnyTermination()