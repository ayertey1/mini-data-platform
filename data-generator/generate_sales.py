# generate_sales.py
import os
import csv
import random
from datetime import date, timedelta, datetime
from faker import Faker
import boto3

fake = Faker()

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
    header = ["sale_date","product_id","product_name","quantity","unit_price"]
    start = date.today() - timedelta(days=30)

    # Add timestamp to filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for _ in range(num_rows):
            sale_date = start + timedelta(days=random.randint(0,30))
            pid = f"P{random.randint(1000,1999)}"
            pname = fake.word().title()
            qty = random.randint(1,10)
            price = round(random.uniform(5,500),2)
            writer.writerow([sale_date.isoformat(), pid, pname, qty, price])
    return filename

def upload_file(filepath):
    s3.Bucket(BUCKET).upload_file(filepath, f"raw/{os.path.basename(filepath)}")
    print(f"Uploaded {filepath} to bucket {BUCKET}/raw/")

if __name__ == "__main__":
    ensure_bucket()
    fp = generate_csv(200, "sales_batch")
    upload_file(fp)
