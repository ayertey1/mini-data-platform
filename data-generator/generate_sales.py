import os
import csv
import random
from datetime import date, timedelta, datetime
import boto3

# --- PRODUCT CATALOG ---
PRODUCT_CATALOG = [
    {"product_id": "P1001", "name": "Laptop - Dell Inspiron 15", "min_price": 450, "max_price": 850},
    {"product_id": "P1002", "name": "Laptop - MacBook Air M2", "min_price": 900, "max_price": 1500},
    {"product_id": "P1003", "name": "Smartphone - iPhone 14", "min_price": 700, "max_price": 1200},
    {"product_id": "P1004", "name": "Smartphone - Samsung Galaxy S23", "min_price": 650, "max_price": 1100},
    {"product_id": "P1005", "name": "Headphones - Sony WH-1000XM5", "min_price": 250, "max_price": 400},
    {"product_id": "P1006", "name": "Tablet - iPad Air", "min_price": 500, "max_price": 800},
    {"product_id": "P1007", "name": "Monitor - LG UltraWide 34\"", "min_price": 350, "max_price": 700},
    {"product_id": "P1008", "name": "Keyboard - Logitech MX Keys", "min_price": 80, "max_price": 150},
    {"product_id": "P1009", "name": "Mouse - Razer DeathAdder V3", "min_price": 50, "max_price": 120},
    {"product_id": "P1010", "name": "Smartwatch - Apple Watch Series 9", "min_price": 350, "max_price": 700},
]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "sales")

s3 = boto3.resource(
    "s3",
    endpoint_url=f"http://{MINIO_ENDPOINT}",
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def ensure_bucket():
    try:
        s3.create_bucket(Bucket=BUCKET)
    except Exception:
        pass

def generate_csv(num_rows=100, filename_prefix="sales"):
    header = ["sale_date", "product_id", "product_name", "quantity", "unit_price"]
    start = date.today() - timedelta(days=30)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for _ in range(num_rows):
            sale_date = start + timedelta(days=random.randint(0, 30))
            product = random.choice(PRODUCT_CATALOG)
            qty = random.randint(1, 10)
            price = round(random.uniform(product["min_price"], product["max_price"]), 2)
            writer.writerow([sale_date.isoformat(), product["product_id"], product["name"], qty, price])
    return filename

def upload_file(filepath):
    s3.Bucket(BUCKET).upload_file(filepath, f"raw/{os.path.basename(filepath)}")
    print(f"Uploaded {filepath} to bucket {BUCKET}/raw/")

if __name__ == "__main__":
    ensure_bucket()
    fp = generate_csv(200, "sales_batch")
    upload_file(fp)
