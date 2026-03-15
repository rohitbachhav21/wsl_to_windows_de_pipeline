# from faker import Faker

# fake = Faker()

# def generate_orders(order_id):
    
#     dt = fake.date_time_between(start_date='-1y', end_date='now')

#     order_date = dt.date()
#     order_time = dt.time()

#     return (order_id, order_date, order_time)


# for i in range(1, 10):

#     order = generate_orders(i)

#     print(order)



# from datetime import datetime, timedelta
# import random

# current_datetime = datetime(2025, 12, 21, 9, 0, 0)

# def generate_orders():

#     global current_datetime

#     # add random minutes between orders
#     current_datetime += timedelta(minutes=random.randint(1, 30))

#     return (current_datetime.date(), current_datetime.time())


from datetime import timedelta
import random

def generate_orders(current_dt):

    current_dt += timedelta(minutes=random.randint(5,25))

    return current_dt, (current_dt.date(), current_dt.time())