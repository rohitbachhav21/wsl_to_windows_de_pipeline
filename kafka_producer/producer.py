import json
import time
import mysql.connector
from kafka import KafkaProducer

# tables to stream
TABLES = {
    "orders": ("pizza_orders", "order_id"),
    "order_details": ("pizza_order_details", "order_details_id")
}

def load_last_id(table):
    try:
        with open(f"{table}_checkpoint.txt") as f:
            return int(f.read().strip())
    except:
        return 0

def save_last_id(table, id):
    with open(f"{table}_checkpoint.txt", "w") as f:
        f.write(str(id))

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="rohit-21",
    database="de_pipeline"
)

cursor = conn.cursor(dictionary=True)

last_ids = {t: load_last_id(t) for t in TABLES}

while True:

    for table, (topic, pk) in TABLES.items():

        query = f"""
        SELECT *
        FROM {table}
        WHERE {pk} > %s
        ORDER BY {pk}
        """

        cursor.execute(query, (last_ids[table],))
        rows = cursor.fetchall()

        for row in rows:
            producer.send(topic, row)
            print(f"{table} → {row}")

            last_ids[table] = row[pk]
            save_last_id(table, last_ids[table])
        
        

    time.sleep(3)