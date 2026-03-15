import json
import time
import mysql.connector
from kafka import KafkaProducer
from loading_last_id import save_last_id

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


connection = mysql.connector.connect(
    # host="10.255.255.254",
    host="localhost",
    user="root",
    password="rohit-21",
    database="de_pipeline"
)

cursor = connection.cursor(dictionary=True)

last_id = 0

while True:

    query = """
    SELECT *
    FROM orders
    WHERE order_id > %s
    ORDER BY order_id
    """

    cursor.execute(query, (last_id,))
    rows = cursor.fetchall()

    for row in rows:
        producer.send("pizza_orders", row)
        print("Sent:", row)

        last_id = row["order_id"]
        save_last_id(last_id)

    time.sleep(3)