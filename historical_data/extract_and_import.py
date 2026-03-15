import zipfile
import os
import pandas as pd
from sqlalchemy import create_engine

# -----------------------------
# CONFIG
# -----------------------------
ZIP_FILE_PATH = "pizza_sales.zip"
EXTRACT_FOLDER = "extracted_data"
INNER_FOLDER = "pizza_sales"   # folder inside the zip

MYSQL_USER = "root"
MYSQL_PASSWORD = "rohit-21"
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DATABASE = "de_pipeline"

# -----------------------------
# MYSQL CONNECTION
# -----------------------------
engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
)

# -----------------------------
# EXTRACT ZIP
# -----------------------------
os.makedirs(EXTRACT_FOLDER, exist_ok=True)

with zipfile.ZipFile(ZIP_FILE_PATH, "r") as zip_ref:
    zip_ref.extractall(EXTRACT_FOLDER)

print("ZIP extracted")

# -----------------------------
# READ CSVs FROM INNER FOLDER
# -----------------------------
csv_path = os.path.join(EXTRACT_FOLDER, INNER_FOLDER)

for file in os.listdir(csv_path):
    if file.endswith(".csv"):
        file_path = os.path.join(csv_path, file)
        table_name = os.path.splitext(file)[0].lower()

        print(f"Importing {file} → {table_name}")

        # Encoding-safe CSV read
        try:
            df = pd.read_csv(file_path, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="latin1")

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False
        )

print("All CSV files imported successfully")

