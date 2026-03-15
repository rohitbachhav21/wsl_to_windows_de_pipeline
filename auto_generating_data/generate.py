import mysql.connector
import time
from datetime import datetime
from orders import generate_orders
from order_details import generate_order_details
from pizza_types import pizza_types
from pizzas import pizzas


connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="rohit-21",
    database="de_pipeline"
)

cursor = connection.cursor()

# Insert pizza_types

query = """
INSERT IGNORE INTO pizza_types (pizza_type_id,name,category,ingredients)
VALUES (%s,%s,%s,%s)
"""

for _,row in pizza_types.iterrows():

    cursor.execute(query,(
        row["pizza_type_id"],
        row["name"],
        row["category"],
        row["ingredients"]
    ))

# Insert pizzas

query = """
INSERT IGNORE INTO pizzas (pizza_id,pizza_type_id,size,price)
VALUES (%s,%s,%s,%s)
"""

for _,row in pizzas.iterrows():

    cursor.execute(query,(
        row["pizza_id"],
        row["pizza_type_id"],
        row["size"],
        row["price"]
    ))

connection.commit()

print("Static tables inserted")



# get last order_id
cursor.execute("SELECT IFNULL(MAX(order_id),0) FROM orders")
order_id = cursor.fetchone()[0] + 1

# get last order_details_id
cursor.execute("SELECT IFNULL(MAX(order_details_id),0) FROM order_details")
order_details_id = cursor.fetchone()[0] + 1

# get last datetime
cursor.execute("""
SELECT date,time
FROM orders
ORDER BY date DESC,time DESC
LIMIT 1
""")

row = cursor.fetchone()

if row:
    current_dt = datetime.strptime(
        f"{row[0]} {row[1]}", "%Y-%m-%d %H:%M:%S"
    )
else:
    current_dt = datetime.now()


while True:

    # generate next order datetime
    current_dt, order = generate_orders(current_dt)

    cursor.execute("""
    INSERT INTO orders (date,time)
    VALUES (%s,%s)
    """, order)

    order_id = cursor.lastrowid

    # generate order details
    details = generate_order_details(order_id)

    cursor.execute("""
    INSERT INTO order_details (order_id,pizza_id,quantity)
    VALUES (%s,%s,%s)
    """, details)

    connection.commit()

    print("Inserted order", order_id)

    time.sleep(2)