# from faker import Faker
# import random

# fake = Faker()

# pizza_ids = [
# "hawaiian_m","classic_dlx_m","five_cheese_l","ital_supr_l","mexicana_m","thai_ckn_l",
# "ital_supr_m","prsc_argla_l","bbq_ckn_s","the_greek_s","spinach_supr_s","classic_dlx_s",
# "green_garden_s","ital_cpcllo_l","ital_supr_s","mexicana_s","spicy_ital_l","spin_pesto_l",
# "veggie_veg_s","mexicana_l","southw_ckn_l","bbq_ckn_l","cali_ckn_l","cali_ckn_m",
# "pepperoni_l","cali_ckn_s","ckn_pesto_l","big_meat_s","soppressata_l","four_cheese_l",
# "napolitana_s","calabrese_m","four_cheese_m","ital_veggie_s","mediterraneo_m",
# "peppr_salami_s","spinach_fet_l","napolitana_l","sicilian_l","ital_cpcllo_m",
# "southw_ckn_s","bbq_ckn_m","pepperoni_m","prsc_argla_s","sicilian_m","veggie_veg_l",
# "ckn_alfredo_s","pepperoni_s","green_garden_l","green_garden_m","pep_msh_pep_l",
# "hawaiian_s","peppr_salami_m","ckn_alfredo_m","peppr_salami_l","spin_pesto_s",
# "thai_ckn_m","classic_dlx_l","ckn_pesto_m","the_greek_xl","hawaiian_l",
# "pep_msh_pep_s","spinach_supr_m","prsc_argla_m","mediterraneo_l","southw_ckn_m",
# "pep_msh_pep_m","sicilian_s","spicy_ital_s","thai_ckn_s","spinach_supr_l",
# "ital_veggie_l","veggie_veg_m","the_greek_m","ckn_pesto_s","spinach_fet_s",
# "spicy_ital_m","ital_veggie_m","ital_cpcllo_s","mediterraneo_s","spinach_fet_m",
# "napolitana_m","spin_pesto_m","brie_carre_s","ckn_alfredo_l","calabrese_s",
# "the_greek_l","soppressata_m","soppressata_s","calabrese_l","the_greek_xxl"
# ]

# def generate_order_details(order_details_id, max_orders=100):

#     order_id = random.randint(1, max_orders)
#     pizza_id = random.choice(pizza_ids)
#     quantity = random.randint(1, 5)

#     return (order_details_id, order_id, pizza_id, quantity)


# for i in range(1,20):

#     row = generate_order_details(i)

#     print(row)


import random
from pizzas import pizzas

pizza_ids = pizzas["pizza_id"].tolist()

def generate_order_details(order_id):

    pizza_id = random.choice(pizza_ids)
    quantity = random.randint(1,5)

    return (order_id,pizza_id,quantity)