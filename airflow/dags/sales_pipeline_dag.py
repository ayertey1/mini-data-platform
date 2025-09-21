from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun, Variable
from airflow.utils import timezone
from datetime import timedelta
import os
import boto3
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
MINIO_HOST = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "sales")

# Get Postgres connection from Airflow's SQL Alchemy connection
POSTGRES_CONN = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description="Process CSV files uploaded to MinIO and load into PostgreSQL",
)

def process_sales_data(**context):
    """
    Process a single CSV file specified by the webhook trigger
    """
    try:
        # Get configuration from DAG run (passed by webhook)
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run else {}
        
        logger.info(f"DAG run configuration: {conf}")
        
        # Extract file information from webhook configuration
        bucket = conf.get("bucket", BUCKET)
        file_key = conf.get("key")  # This is the decoded key from webhook
        triggered_by = conf.get("triggered_by", "unknown")
        
        if not file_key:
            raise ValueError("No file key provided in dag_run.conf")
        
        logger.info(f"Processing file: {bucket}/{file_key} (triggered by: {triggered_by})")
        
        # Initialize MinIO client
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}",
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        
        # Initialize PostgreSQL connection
        if not POSTGRES_CONN:
            raise ValueError("POSTGRES_CONN environment variable not set")
        
        engine = create_engine(POSTGRES_CONN)
        
        # Download and read the CSV file
        logger.info(f"Downloading file from MinIO: {file_key}")
        try:
            resp = s3.get_object(Bucket=bucket, Key=file_key)
            body = resp["Body"].read()
            df = pd.read_csv(BytesIO(body))
            logger.info(f"Successfully read CSV file with {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to download or parse CSV file: {str(e)}")
            raise
        
        # Data validation and cleaning
        expected_cols = ["sale_date", "product_id", "product_name", "quantity", "unit_price"]
        
        # Check if all expected columns are present
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}. Found columns: {df.columns.tolist()}")
        
        # Select and clean the data
        df = df[expected_cols].copy()
        
        # Data type conversions and cleaning
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0)
        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
        df["total_amount"] = df["quantity"] * df["unit_price"]
        
        # Remove rows with invalid dates
        df = df.dropna(subset=["sale_date"])
        
        logger.info(f"Data cleaned. Final dataset has {len(df)} rows")
        
        # Insert into PostgreSQL
        try:
            df.to_sql(
                "sales", 
                engine, 
                if_exists="append", 
                index=False, 
                method="multi"
            )
            logger.info(f"Successfully inserted {len(df)} rows into PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to insert data into PostgreSQL: {str(e)}")
            raise
        
        # Move processed file to processed/ folder
        try:
            filename = file_key.split("/")[-1]  # Extract filename from path
            processed_key = f"processed/{filename}"
            
            # Copy file to processed folder
            s3.copy_object(
                Bucket=bucket, 
                CopySource={"Bucket": bucket, "Key": file_key}, 
                Key=processed_key
            )
            logger.info(f"File copied to: {processed_key}")
            
            # Delete original file
            s3.delete_object(Bucket=bucket, Key=file_key)
            logger.info(f"Original file deleted: {file_key}")
            
        except Exception as e:
            logger.error(f"Failed to move file to processed folder: {str(e)}")
            # Don't raise here - data is already processed successfully
            logger.warning("File processing completed but file movement failed")
        
        logger.info("Sales data processing completed successfully")
        
        return {
            "status": "success",
            "processed_file": file_key,
            "rows_processed": len(df),
            "moved_to": f"processed/{filename}" if 'filename' in locals() else None
        }
        
    except Exception as e:
        logger.error(f"Error processing sales data: {str(e)}")
        raise

# Create the task
process_task = PythonOperator(
    task_id="process_sales_data",
    python_callable=process_sales_data,
    provide_context=True,
    dag=dag,
)