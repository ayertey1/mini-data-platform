from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import DagRun, Variable
from airflow.utils import timezone
from datetime import datetime, timedelta
import os
import boto3
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO, StringIO
import logging
import json
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
MINIO_HOST = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET = os.getenv("MINIO_BUCKET", "sales")
POSTGRES_CONN = os.getenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN")

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}

# Main pipeline DAG (event-driven)
dag_main = DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # Event-driven
    catchup=False,
    max_active_runs=1,
    description="Process CSV files uploaded to MinIO and load into PostgreSQL (Event-driven)",
)

# Cleanup/Recovery DAG (scheduled)
dag_cleanup = DAG(
    dag_id="sales_pipeline_cleanup",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="59 23 * * *",  # 11:59 PM daily
    catchup=False,
    max_active_runs=1,
    description="Process any missed CSV files in MinIO bucket",
)

def write_log_to_minio(s3_client, log_content, file_key, status, error_msg=None):
    """Write pipeline logs to MinIO"""
    try:
        log_date = datetime.now().strftime("%Y-%m-%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "file": file_key,
            "status": status,
            "error": error_msg,
            "log_content": log_content
        }
        
        log_key = f"logs/{log_date}/pipeline_log_{timestamp}_{status}.json"
        
        s3_client.put_object(
            Bucket=BUCKET,
            Key=log_key,
            Body=json.dumps(log_data, indent=2),
            ContentType="application/json"
        )
        
        logger.info(f"Log written to MinIO: {log_key}")
        return log_key
    except Exception as e:
        logger.error(f"Failed to write log to MinIO: {str(e)}")
        return None

def get_config(**context):
    """Stage 1: Extract and validate configuration"""
    try:
        dag_run = context.get("dag_run")
        conf = dag_run.conf if dag_run else {}
        
        logger.info(f"DAG run configuration: {conf}")
        
        # For cleanup DAG, we'll process all files in raw/
        if context['dag'].dag_id == "sales_pipeline_cleanup":
            config = {
                "bucket": BUCKET,
                "file_key": None,  # Will be determined in download stage
                "triggered_by": "scheduled_cleanup",
                "is_cleanup": True
            }
        else:
            # Event-driven pipeline
            bucket = conf.get("bucket", BUCKET)
            file_key = conf.get("key")
            triggered_by = conf.get("triggered_by", "unknown")
            
            if not file_key:
                raise ValueError("No file key provided in dag_run.conf")
            
            config = {
                "bucket": bucket,
                "file_key": file_key,
                "triggered_by": triggered_by,
                "is_cleanup": False
            }
        
        logger.info(f"Configuration validated: {config}")
        
        # Store config for downstream tasks
        context['task_instance'].xcom_push(key='config', value=config)
        
        return config
        
    except Exception as e:
        logger.error(f"Configuration error: {str(e)}")
        raise

def init_clients(**context):
    """Stage 2: Initialize MinIO and PostgreSQL clients"""
    try:
        config = context['task_instance'].xcom_pull(key='config', task_ids='get_config')
        
        logger.info("Initializing clients...")
        
        # Initialize MinIO client
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}",
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        
        # Test MinIO connection
        s3.list_objects_v2(Bucket=config['bucket'], MaxKeys=1)
        logger.info("MinIO client initialized successfully")
        
        # Initialize PostgreSQL connection
        if not POSTGRES_CONN:
            raise ValueError("POSTGRES_CONN environment variable not set")
        
        engine = create_engine(POSTGRES_CONN)
        
        # Test PostgreSQL connection
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info("PostgreSQL client initialized successfully")
        
        # Store clients info for downstream tasks
        context['task_instance'].xcom_push(key='clients_ready', value=True)
        
        return {"minio": "ready", "postgresql": "ready"}
        
    except Exception as e:
        logger.error(f"Client initialization error: {str(e)}")
        raise

def download_file(**context):
    """Stage 3: Download and list files for processing"""
    try:
        config = context['task_instance'].xcom_pull(key='config', task_ids='get_config')
        
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}",
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        
        files_to_process = []
        
        if config['is_cleanup']:
            # Cleanup mode: find all CSV files in raw/
            logger.info("Cleanup mode: scanning for unprocessed files...")
            
            try:
                response = s3.list_objects_v2(Bucket=config['bucket'], Prefix="raw/")
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.lower().endswith('.csv') and key != "raw/":
                            files_to_process.append(key)
                            
                logger.info(f"Found {len(files_to_process)} files for cleanup processing")
            except Exception as e:
                logger.error(f"Failed to list files for cleanup: {str(e)}")
                raise
        else:
            # Event-driven mode: process specific file
            file_key = config['file_key']
            logger.info(f"Event mode: processing file {file_key}")
            files_to_process = [file_key]
        
        if not files_to_process:
            logger.info("No files to process - this is normal for cleanup runs when no new files are present")
            # Store empty list explicitly
            context['task_instance'].xcom_push(key='files_to_process', value=[])
            return {"files_processed": 0, "message": "No files found to process"}
        
        # Store files for downstream processing
        context['task_instance'].xcom_push(key='files_to_process', value=files_to_process)
        
        return {"files_to_process": files_to_process, "files_found": len(files_to_process)}
        
    except Exception as e:
        logger.error(f"File download/listing error: {str(e)}")
        raise

def validate_clean(**context):
    """Stage 4: Validate and clean data"""
    try:
        config = context['task_instance'].xcom_pull(key='config', task_ids='get_config')
        files_to_process = context['task_instance'].xcom_pull(key='files_to_process', task_ids='download_file')
        
        # Handle case where no files are present (common in cleanup runs)
        if not files_to_process or len(files_to_process) == 0:
            logger.info("No files to validate and clean - skipping this stage")
            # Store empty results for downstream tasks
            context['task_instance'].xcom_push(key='processed_files', value=[])
            context['task_instance'].xcom_push(key='rejected_files', value=[])
            return {
                "processed_files": 0,
                "rejected_files": 0,
                "message": "No files to process"
            }
        
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}",
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        
        expected_cols = ["sale_date", "product_id", "product_name", "quantity", "unit_price"]
        processed_files = []
        rejected_files = []
        
        logger.info(f"Starting validation and cleaning for {len(files_to_process)} files")
        
        for file_key in files_to_process:
            try:
                logger.info(f"Validating and cleaning file: {file_key}")
                
                # Download file
                resp = s3.get_object(Bucket=config['bucket'], Key=file_key)
                body = resp["Body"].read()
                df = pd.read_csv(BytesIO(body))
                
                logger.info(f"File {file_key} loaded with {len(df)} rows and columns: {df.columns.tolist()}")
                
                # Check if all expected columns are present
                missing_cols = [col for col in expected_cols if col not in df.columns]
                
                if missing_cols:
                    # Move to rejected folder
                    filename = file_key.split("/")[-1]
                    rejected_key = f"rejected/{filename}"
                    
                    s3.copy_object(
                        Bucket=config['bucket'], 
                        CopySource={"Bucket": config['bucket'], "Key": file_key}, 
                        Key=rejected_key
                    )
                    s3.delete_object(Bucket=config['bucket'], Key=file_key)
                    
                    error_msg = f"Missing required columns: {missing_cols}. Found columns: {df.columns.tolist()}"
                    logger.warning(f"File {file_key} rejected: {error_msg}")
                    
                    # Write rejection log
                    log_content = f"File rejected due to schema validation failure: {error_msg}"
                    write_log_to_minio(s3, log_content, file_key, "REJECTED", error_msg)
                    
                    rejected_files.append({
                        "file": file_key,
                        "reason": error_msg,
                        "moved_to": rejected_key
                    })
                    continue
                
                # Data validation and cleaning
                df = df[expected_cols].copy()
                
                # Data type conversions and cleaning
                original_rows = len(df)
                df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(0).astype(int)
                df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0.0)
                df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
                df["total_amount"] = df["quantity"] * df["unit_price"]
                
                # Remove rows with invalid dates
                df = df.dropna(subset=["sale_date"])
                
                cleaned_rows = len(df)
                logger.info(f"File {file_key} cleaned: {original_rows} -> {cleaned_rows} rows")
                
                # Store cleaned data
                processed_files.append({
                    "file_key": file_key,
                    "dataframe": df,
                    "original_rows": original_rows,
                    "cleaned_rows": cleaned_rows
                })
                
            except Exception as e:
                logger.error(f"Error processing file {file_key}: {str(e)}")
                
                # Move to rejected folder
                try:
                    filename = file_key.split("/")[-1]
                    rejected_key = f"rejected/{filename}"
                    
                    s3.copy_object(
                        Bucket=config['bucket'], 
                        CopySource={"Bucket": config['bucket'], "Key": file_key}, 
                        Key=rejected_key
                    )
                    s3.delete_object(Bucket=config['bucket'], Key=file_key)
                    
                    # Write error log
                    log_content = f"File processing error: {str(e)}\n{traceback.format_exc()}"
                    write_log_to_minio(s3, log_content, file_key, "ERROR", str(e))
                    
                    rejected_files.append({
                        "file": file_key,
                        "reason": str(e),
                        "moved_to": rejected_key
                    })
                except Exception as move_error:
                    logger.error(f"Failed to move error file {file_key}: {str(move_error)}")
        
        # Store results for downstream tasks
        context['task_instance'].xcom_push(key='processed_files', value=processed_files)
        context['task_instance'].xcom_push(key='rejected_files', value=rejected_files)
        
        logger.info(f"Validation completed: {len(processed_files)} processed, {len(rejected_files)} rejected")
        
        return {
            "processed_files": len(processed_files),
            "rejected_files": len(rejected_files)
        }
        
    except Exception as e:
        logger.error(f"Validation/cleaning error: {str(e)}")
        raise

def insert_postgres(**context):
    """Stage 5: Insert data into PostgreSQL"""
    try:
        config = context['task_instance'].xcom_pull(key='config', task_ids='get_config')
        processed_files = context['task_instance'].xcom_pull(key='processed_files', task_ids='validate_clean')
        
        # Handle case where no files were processed
        if not processed_files or len(processed_files) == 0:
            logger.info("No files to insert into PostgreSQL - skipping this stage")
            context['task_instance'].xcom_push(key='total_rows_inserted', value=0)
            return {
                "rows_inserted": 0,
                "message": "No processed files to insert"
            }
        
        engine = create_engine(POSTGRES_CONN)
        total_rows_inserted = 0
        
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}",
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        
        logger.info(f"Starting database insertion for {len(processed_files)} files")
        
        for file_info in processed_files:
            try:
                df = file_info['dataframe']
                file_key = file_info['file_key']
                
                logger.info(f"Inserting {len(df)} rows from {file_key} into PostgreSQL")
                
                df.to_sql(
                    "sales", 
                    engine, 
                    if_exists="append", 
                    index=False, 
                    method="multi"
                )
                
                total_rows_inserted += len(df)
                logger.info(f"Successfully inserted {len(df)} rows from {file_key}")
                
                # Write success log
                log_content = f"Successfully inserted {len(df)} rows from {file_key} into PostgreSQL"
                write_log_to_minio(s3, log_content, file_key, "SUCCESS")
                
            except Exception as e:
                logger.error(f"Failed to insert data from {file_info['file_key']}: {str(e)}")
                
                # Write error log
                log_content = f"Database insertion failed: {str(e)}\n{traceback.format_exc()}"
                write_log_to_minio(s3, log_content, file_info['file_key'], "DB_ERROR", str(e))
                raise
        
        # Store result for downstream tasks
        context['task_instance'].xcom_push(key='total_rows_inserted', value=total_rows_inserted)
        
        logger.info(f"Database insertion completed: {total_rows_inserted} total rows inserted")
        
        return {"rows_inserted": total_rows_inserted}
        
    except Exception as e:
        logger.error(f"PostgreSQL insertion error: {str(e)}")
        raise

def move_file(**context):
    """Stage 6: Move processed files to processed/ folder"""
    try:
        config = context['task_instance'].xcom_pull(key='config', task_ids='get_config')
        processed_files = context['task_instance'].xcom_pull(key='processed_files', task_ids='validate_clean')
        total_rows_inserted = context['task_instance'].xcom_pull(key='total_rows_inserted', task_ids='insert_postgres')
        
        # Handle case where no files were processed
        if not processed_files or len(processed_files) == 0:
            logger.info("No files to move - cleanup run completed with no files to process")
            return {
                "files_moved": 0,
                "total_rows_processed": 0,
                "message": "No files to move"
            }
        
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_HOST}",
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        
        files_moved = 0
        
        logger.info(f"Starting file movement for {len(processed_files)} files")
        
        for file_info in processed_files:
            try:
                file_key = file_info['file_key']
                filename = file_key.split("/")[-1]
                processed_key = f"processed/{filename}"
                
                # Copy to processed folder
                s3.copy_object(
                    Bucket=config['bucket'], 
                    CopySource={"Bucket": config['bucket'], "Key": file_key}, 
                    Key=processed_key
                )
                
                # Delete original
                s3.delete_object(Bucket=config['bucket'], Key=file_key)
                
                logger.info(f"Moved {file_key} to {processed_key}")
                files_moved += 1
                
                # Write completion log
                log_content = f"Pipeline completed successfully. File moved from {file_key} to {processed_key}. Rows processed: {file_info['cleaned_rows']}"
                write_log_to_minio(s3, log_content, file_key, "COMPLETED")
                
            except Exception as e:
                logger.error(f"Failed to move file {file_info['file_key']}: {str(e)}")
                # Don't raise here - data is already processed successfully
                logger.warning("File processing completed but file movement failed")
        
        logger.info(f"File movement completed: {files_moved} files moved, {total_rows_inserted or 0} total rows processed")
        
        return {
            "files_moved": files_moved,
            "total_rows_processed": total_rows_inserted or 0
        }
        
    except Exception as e:
        logger.error(f"File movement error: {str(e)}")
        # Don't raise here - this is not critical if data is already inserted
        logger.warning("File movement failed but pipeline completed successfully")
        return {"files_moved": 0, "error": str(e), "total_rows_processed": 0}

# Define tasks for main pipeline
start_main = DummyOperator(task_id='start', dag=dag_main)

get_config_main = PythonOperator(
    task_id='get_config',
    python_callable=get_config,
    dag=dag_main,
)

init_clients_main = PythonOperator(
    task_id='init_clients',
    python_callable=init_clients,
    dag=dag_main,
)

download_file_main = PythonOperator(
    task_id='download_file',
    python_callable=download_file,
    dag=dag_main,
)

validate_clean_main = PythonOperator(
    task_id='validate_clean',
    python_callable=validate_clean,
    dag=dag_main,
)

insert_postgres_main = PythonOperator(
    task_id='insert_postgres',
    python_callable=insert_postgres,
    dag=dag_main,
)

move_file_main = PythonOperator(
    task_id='move_file',
    python_callable=move_file,
    dag=dag_main,
)

end_main = DummyOperator(task_id='end', dag=dag_main)

# Define task dependencies for main pipeline
start_main >> get_config_main >> init_clients_main >> download_file_main >> validate_clean_main >> insert_postgres_main >> move_file_main >> end_main

# Define tasks for cleanup pipeline
start_cleanup = DummyOperator(task_id='start', dag=dag_cleanup)

get_config_cleanup = PythonOperator(
    task_id='get_config',
    python_callable=get_config,
    dag=dag_cleanup,
)

init_clients_cleanup = PythonOperator(
    task_id='init_clients',
    python_callable=init_clients,
    dag=dag_cleanup,
)

download_file_cleanup = PythonOperator(
    task_id='download_file',
    python_callable=download_file,
    dag=dag_cleanup,
)

validate_clean_cleanup = PythonOperator(
    task_id='validate_clean',
    python_callable=validate_clean,
    dag=dag_cleanup,
)

insert_postgres_cleanup = PythonOperator(
    task_id='insert_postgres',
    python_callable=insert_postgres,
    dag=dag_cleanup,
)

move_file_cleanup = PythonOperator(
    task_id='move_file',
    python_callable=move_file,
    dag=dag_cleanup,
)

end_cleanup = DummyOperator(task_id='end', dag=dag_cleanup)

# Define task dependencies for cleanup pipeline
start_cleanup >> get_config_cleanup >> init_clients_cleanup >> download_file_cleanup >> validate_clean_cleanup >> insert_postgres_cleanup >> move_file_cleanup >> end_cleanup