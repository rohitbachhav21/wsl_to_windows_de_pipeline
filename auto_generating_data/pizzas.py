
import pandas as pd

base_price = {
"bbq_ckn": 11.5,
"cali_ckn": 12.5,
"ckn_alfredo": 12.0,
"ckn_pesto": 12.0,
"southw_ckn": 12.5,
"thai_ckn": 13.0,
"big_meat": 13.5,
"classic_dlx": 12.5,
"hawaiian": 11.5,
"ital_cpcllo": 13.5,
"napolitana": 11.0,
"pep_msh_pep": 11.5,
"pepperoni": 10.5,
"the_greek": 13.0,
"brie_carre": 14.0,
"calabrese": 13.5,
"ital_supr": 13.5,
"peppr_salami": 13.0,
"prsc_argla": 14.0,
"sicilian": 13.5,
"soppressata": 13.0,
"spicy_ital": 13.5,
"spinach_supr": 12.5,
"five_cheese": 12.0,
"four_cheese": 12.0,
"green_garden": 11.0,
"ital_veggie": 11.5,
"mediterraneo": 12.0,
"mexicana": 12.5,
"spin_pesto": 11.5,
"spinach_fet": 11.0,
"veggie_veg": 10.5
}

size_increment = {
"S": 0,
"M": 4,
"L": 8,
"XL": 11,
"XXL": 14
}


sizes = ["S","M","L","XL","XXL"]

rows = []

for pizza_type, base in base_price.items():

    for size in sizes:

        price = round(base + size_increment[size], 2)

        rows.append({
            "pizza_id": f"{pizza_type}_{size.lower()}",
            "pizza_type_id": pizza_type,
            "size": size,
            "price": price
        })

pizzas = pd.DataFrame(rows)

print(pizzas)