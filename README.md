# MinIO Webhook Integration with Airflow - Complete Documentation

## Overview
This document details the complete setup process for integrating MinIO object storage with Apache Airflow using webhook notifications. The system automatically triggers Airflow DAGs when CSV files are uploaded to specific MinIO buckets.

## Architecture
- **MinIO**: Object storage service that sends webhook notifications on file uploads
- **Webhook Service**: FastAPI application that receives MinIO events and calls Airflow API
- **Airflow**: Workflow orchestration platform that executes data pipelines
- **PostgreSQL**: Database for Airflow metadata and Metabase
- **Metabase**: Business intelligence tool for data visualization

## Initial Problem
The original issue was that MinIO environment variables for webhook configuration were not being passed to the MinIO container, preventing webhook notifications from being configured.

## Project Structure
```
mini-data-platform/
├── docker-compose.yml
├── webhook/
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
├── airflow/
│   ├── dags/
│   │   └── sales_pipeline_dag.py
│   └── requirements.txt
├── infra/
│   └── postgres/
│       ├── init_db.sql
│       └── init_metabase.sql
└── data-generator/
    └── generate_sales.py
```

## Issues Encountered and Solutions

### Issue 1: MinIO Environment Variables Not Loading

**Problem**: Environment variables defined in docker-compose.yml were not appearing in the MinIO container runtime.

**Root Cause**: YAML mapping format (`KEY: "value"`) was not being processed correctly by Docker Compose.

**Solution**: Changed environment variable format from YAML mapping to YAML array format:

```yaml
# WRONG - Mapping format
environment:
  MINIO_NOTIFY_WEBHOOK_AIRFLOW_ENABLE: "on"
  MINIO_NOTIFY_WEBHOOK_AIRFLOW_ENDPOINT: "http://webhook:8000/minio-webhook"

# CORRECT - Array format
environment:
  - MINIO_NOTIFY_WEBHOOK_AIRFLOW_ENABLE=on
  - MINIO_NOTIFY_WEBHOOK_AIRFLOW_ENDPOINT=http://webhook:8000/minio-webhook
```

**Verification Command**:
```bash
docker-compose exec minio printenv | grep MINIO_NOTIFY
```

### Issue 2: Webhook Container Health Check Failures

**Problem**: Webhook container was marked as "unhealthy" by Docker Compose, causing dependency failures.

**Root Cause**: Missing `/health` endpoint in the FastAPI application.

**Solution**: Added health check endpoint to the webhook service:

```python
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "minio-webhook",
        "airflow_url": AIRFLOW_API_URL,
        "target_dag": TARGET_DAG_ID
    }
```

**Temporary Workaround**: Removed health check dependencies from docker-compose.yml to allow services to start:

```yaml
webhook:
  # Removed: healthcheck and condition dependencies
  depends_on:
    - airflow-webserver  # Simple dependency without health check
```

### Issue 3: MinIO Webhook Configuration Not Applied

**Problem**: Even with correct environment variables, MinIO's `notify_webhook` configuration showed as disabled.

**Root Cause**: MinIO environment variables alone are not sufficient; the webhook target must be configured through MinIO's admin interface.

**Solution**: Manually configured webhook using MinIO Client (mc):

```bash
# Set up MinIO alias
docker-compose exec mc mc alias set local http://minio:9000 minioadmin minioadmin

# Configure webhook notification target
docker-compose exec mc mc admin config set local notify_webhook:airflow endpoint='http://webhook:8000/minio-webhook' auth_token='supersecret' queue_limit='10'

# Restart MinIO to apply configuration
docker-compose exec mc mc admin service restart local

# Create bucket and add event notification
docker-compose exec mc mc mb -p local/sales
docker-compose exec mc mc event add local/sales arn:minio:sqs::airflow:webhook --event put --prefix raw/ --suffix .csv
```

**Verification Commands**:
```bash
# Check webhook configuration
docker-compose exec mc mc admin config get local/ notify_webhook:airflow

# List event notifications
docker-compose exec mc mc event list local/sales
```

### Issue 4: MC Container Startup Script Hanging

**Problem**: The MC container's startup script got stuck in an infinite loop waiting for MinIO health checks.

**Root Cause**: Health check URL or network connectivity issues between MC and MinIO containers.

**Solution**: Used direct container execution instead of automated startup script:

```bash
# Start MC container manually
docker-compose up mc -d

# Execute commands directly in the container
docker-compose exec mc sh
```

### Issue 5: Webhook Not Triggering Airflow DAGs

**Problem**: Webhook received MinIO events successfully but did not trigger Airflow DAGs.

**Root Cause**: MinIO sends object keys URL-encoded (e.g., `raw%2Ffile.csv` instead of `raw/file.csv`), causing the string matching logic to fail.

**Original Failing Code**:
```python
object_key = record.get("s3", {}).get("object", {}).get("key", "")
if object_key.startswith("raw/"):  # This failed with "raw%2Ffile.csv"
```

**Solution**: Added URL decoding before string matching:

```python
from urllib.parse import unquote

object_key = record.get("s3", {}).get("object", {}).get("key", "")
decoded_key = unquote(object_key)  # Convert "raw%2Ffile.csv" to "raw/file.csv"
if decoded_key.startswith("raw/"):  # Now works correctly
```

### Issue 6: Airflow DAG Paused by Default

**Problem**: Even when webhook successfully called Airflow API, DAGs were paused and wouldn't execute.

**Root Cause**: Airflow DAGs are paused by default when created.

**Solution**: Unpause the DAG using Airflow API:

```powershell
$headers = @{
    'Content-Type' = 'application/json'
    'Authorization' = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('admin:admin'))
}

Invoke-RestMethod -Uri "http://localhost:8080/api/v1/dags/sales_pipeline" -Method PATCH -Headers $headers -Body '{"is_paused": false}'
```

## Complete Working Configuration

### docker-compose.yml
```yaml
version: "3.8"

services:
  postgres:
    image: postgres:15
    restart: always
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-airflow}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-airflow}
      POSTGRES_DB: ${POSTGRES_DB:-airflow}
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-airflow} -d ${POSTGRES_DB:-airflow}"]
      interval: 10s
      timeout: 5s
      retries: 5
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./infra/postgres/init_db.sql:/docker-entrypoint-initdb.d/init_db.sql:ro
      - ./infra/postgres/init_metabase.sql:/docker-entrypoint-initdb.d/init_metabase.sql:ro
    networks:
      - mdp-network

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    environment:
      - MINIO_ROOT_USER=${MINIO_ROOT_USER:-minioadmin}
      - MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-minioadmin}
      - MINIO_NOTIFY_WEBHOOK_AIRFLOW_ENABLE=on
      - MINIO_NOTIFY_WEBHOOK_AIRFLOW_ENDPOINT=http://webhook:8000/minio-webhook
      - MINIO_NOTIFY_WEBHOOK_AIRFLOW_AUTH_TOKEN=supersecret
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/ready"]
      interval: 10s
      retries: 6
    networks:
      - mdp-network

  webhook:
    build:
      context: ./webhook
      dockerfile: Dockerfile
    environment:
      - WEBHOOK_PORT=8000
      - AIRFLOW_API_URL=http://airflow-webserver:8080/api/v1
      - AIRFLOW_USER=${AIRFLOW_API_USER:-admin}
      - AIRFLOW_PASS=${AIRFLOW_API_PASS:-admin}
      - TARGET_DAG_ID=${TARGET_DAG_ID:-sales_pipeline}
      - AUTH_TOKEN=supersecret
    ports:
      - "8000:8000"
    depends_on:
      - airflow-webserver
    networks:
      - mdp-network

  mc:
    image: minio/mc
    depends_on:
      - minio
    entrypoint: >
      /bin/sh -c "
      echo 'Waiting for MinIO to be ready...';
      until curl -s http://minio:9000/minio/health/ready >/dev/null 2>&1; do echo 'Waiting for MinIO...'; sleep 2; done;
      echo 'Setting up MinIO alias...';
      mc alias set local http://minio:9000 ${MINIO_ROOT_USER:-minioadmin} ${MINIO_ROOT_PASSWORD:-minioadmin};
      echo 'Configuring webhook notification...';
      mc admin config set local notify_webhook:airflow endpoint='http://webhook:8000/minio-webhook' auth_token='supersecret' queue_limit='10';
      echo 'Restarting MinIO service to apply webhook config...';
      mc admin service restart local;
      echo 'Waiting for MinIO to restart...';
      sleep 10;
      until curl -s http://minio:9000/minio/health/ready >/dev/null 2>&1; do echo 'Waiting for MinIO restart...'; sleep 2; done;
      echo 'Creating bucket...';
      mc mb -p local/sales || true;
      echo 'Adding event notification...';
      mc event add local/sales arn:minio:sqs::airflow:webhook --event put --prefix raw/ --suffix .csv || true;
      echo 'Current events:';
      mc event list local/sales || true;
      echo 'MinIO configuration complete. Keeping container running...';
      tail -f /dev/null
      "
    networks:
      - mdp-network

  airflow-webserver:
    image: apache/airflow:2.6.3
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@postgres:5432/${POSTGRES_DB:-airflow}
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW__FERNET_KEY}
      AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX: "True"
      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: "True"
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      AIRFLOW__API__AUTH_BACKENDS: "airflow.api.auth.backend.basic_auth"
      AWS_ACCESS_KEY_ID: ${MINIO_ACCESS_KEY:-minioadmin}
      AWS_SECRET_ACCESS_KEY: ${MINIO_SECRET_KEY:-minioadmin}
    depends_on:
      postgres:
        condition: service_healthy
      minio:
        condition: service_healthy
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/requirements.txt:/requirements.txt:ro
    ports:
      - "8080:8080"
    command: >
      bash -c "pip install -r /requirements.txt &&
               airflow db upgrade &&
               airflow users create --username ${AIRFLOW_API_USER:-admin} --password ${AIRFLOW_API_PASS:-admin} --firstname Admin --lastname User --role Admin --email admin@example.com || true &&
               exec airflow webserver -p 8080"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 10s
      retries: 6
      start_period: 40s
    networks:
      - mdp-network

  airflow-scheduler:
    image: apache/airflow:2.6.3
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://${POSTGRES_USER:-airflow}:${POSTGRES_PASSWORD:-airflow}@postgres:5432/${POSTGRES_DB:-airflow}
      AIRFLOW__CORE__FERNET_KEY: ${AIRFLOW__FERNET_KEY}
    depends_on:
      - airflow-webserver
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/requirements.txt:/requirements.txt:ro
    command: >
      bash -c "pip install -r /requirements.txt &&
               airflow scheduler"
    networks:
      - mdp-network

  metabase:
    image: metabase/metabase:latest
    environment:
      MB_DB_TYPE: postgres
      MB_DB_HOST: postgres
      MB_DB_PORT: 5432
      MB_DB_DBNAME: ${METABASE_DB:-metabase}
      MB_DB_USER: ${METABASE_USER:-metabase}
      MB_DB_PASS: ${METABASE_PASSWORD:-metabase}
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - mdp-network

volumes:
  pgdata:
  minio_data:

networks:
  mdp-network:
    driver: bridge
```

### webhook/app.py
```python
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import requests
import os
import logging
from typing import Dict, Any
import json
from urllib.parse import unquote

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="MinIO Webhook Service", version="1.0.0")

# Configuration from environment variables
WEBHOOK_PORT = os.getenv("WEBHOOK_PORT", "8000")
AIRFLOW_API_URL = os.getenv("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin")
TARGET_DAG_ID = os.getenv("TARGET_DAG_ID", "sales_pipeline")
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "supersecret")

# Security
security = HTTPBearer()

def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify the authorization token"""
    if credentials.credentials != AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    return credentials.credentials

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "MinIO Webhook Service",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint for Docker"""
    try:
        return {
            "status": "healthy",
            "service": "minio-webhook",
            "airflow_url": AIRFLOW_API_URL,
            "target_dag": TARGET_DAG_ID
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unhealthy")

@app.post("/minio-webhook")
async def minio_webhook(request: Request, token: str = Depends(verify_token)):
    """Handle MinIO webhook notifications"""
    try:
        # Get the JSON payload from MinIO
        payload = await request.json()
        logger.info(f"Received MinIO webhook payload: {json.dumps(payload, indent=2)}")
        
        if "Records" in payload:
            for record in payload["Records"]:
                event_name = record.get("eventName", "")
                bucket_name = record.get("s3", {}).get("bucket", {}).get("name", "")
                object_key = record.get("s3", {}).get("object", {}).get("key", "")
                
                # URL decode the object key (MinIO sends it URL-encoded)
                decoded_key = unquote(object_key)
                
                logger.info(f"Processing event: {event_name} for {bucket_name}/{object_key}")
                logger.info(f"Decoded key: {decoded_key}")
                
                # Check if this is a CSV file upload to the raw/ prefix
                if (event_name.startswith("s3:ObjectCreated") and 
                    decoded_key.startswith("raw/") and 
                    decoded_key.endswith(".csv")):
                    
                    logger.info(f"Matching CSV file detected, triggering Airflow DAG...")
                    
                    # Trigger Airflow DAG
                    dag_run_result = await trigger_airflow_dag(
                        dag_id=TARGET_DAG_ID,
                        conf={
                            "bucket": bucket_name,
                            "key": decoded_key,
                            "original_key": object_key,
                            "event_name": event_name,
                            "triggered_by": "minio_webhook"
                        }
                    )
                    
                    logger.info(f"DAG triggered successfully: {dag_run_result}")
                else:
                    logger.info(f"File does not match criteria - Event: {event_name}, Key: {decoded_key}")
        
        return {
            "status": "success",
            "message": "Webhook processed successfully",
            "processed_records": len(payload.get("Records", []))
        }
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {str(e)}")

async def trigger_airflow_dag(dag_id: str, conf: Dict[str, Any] = None):
    """Trigger an Airflow DAG via REST API"""
    try:
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"
        
        logger.info(f"Attempting to trigger DAG {dag_id} at {url}")
        logger.info(f"DAG configuration: {conf}")
        
        # Prepare the payload for Airflow API
        airflow_payload = {
            "conf": conf or {}
        }
        
        # Make request to Airflow API with basic auth
        response = requests.post(
            url,
            json=airflow_payload,
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        logger.info(f"Airflow API response status: {response.status_code}")
        
        if response.status_code in [200, 201]:
            logger.info(f"Successfully triggered DAG {dag_id}")
            return response.json()
        else:
            logger.error(f"Failed to trigger DAG {dag_id}: {response.status_code} - {response.text}")
            raise HTTPException(
                status_code=response.status_code, 
                detail=f"Failed to trigger Airflow DAG: {response.text}"
            )
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error when calling Airflow API: {str(e)}")
        raise HTTPException(status_code=503, detail=f"Airflow API unreachable: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(WEBHOOK_PORT))
```

### webhook/requirements.txt
```
fastapi==0.104.1
uvicorn[standard]==0.24.0
requests==2.31.0
python-multipart==0.0.6
```

### webhook/Dockerfile
```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better Docker layer caching
COPY ./requirements.txt /app/requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the application code
COPY ./app.py /app/app.py

# Expose port
EXPOSE 8000

# Run the application
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

## Step-by-Step Setup Process

### 1. Environment Preparation
```bash
# Create project directory
mkdir mini-data-platform
cd mini-data-platform

# Create directory structure
mkdir -p webhook airflow/dags infra/postgres data-generator
```

### 2. Create Configuration Files
Create all the files listed in the "Complete Working Configuration" section above.

### 3. Build and Start Services
```bash
# Start all services
docker-compose up --build -d

# Monitor service startup
docker-compose ps
```

### 4. Manual MinIO Webhook Configuration
If the automated MC configuration fails:

```bash
# Start MC container
docker-compose up mc -d

# Execute configuration manually
docker-compose exec mc sh

# Inside MC container:
mc alias set local http://minio:9000 minioadmin minioadmin
mc admin config set local notify_webhook:airflow endpoint='http://webhook:8000/minio-webhook' auth_token='supersecret' queue_limit='10'
mc admin service restart local
mc mb -p local/sales
mc event add local/sales arn:minio:sqs::airflow:webhook --event put --prefix raw/ --suffix .csv
mc event list local/sales
exit
```

### 5. Unpause Airflow DAG
```powershell
# PowerShell command
$headers = @{
    'Content-Type' = 'application/json'
    'Authorization' = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('admin:admin'))
}

Invoke-RestMethod -Uri "http://localhost:8080/api/v1/dags/sales_pipeline" -Method PATCH -Headers $headers -Body '{"is_paused": false}'
```

### 6. Test the Integration
```bash
# Upload a test file
python data-generator/generate_sales.py

# Check webhook logs
docker-compose logs webhook --tail 20

# Check Airflow DAG runs
# Visit http://localhost:8080 or use API
```

## Verification Commands

### Check MinIO Environment Variables
```bash
docker-compose exec minio printenv | grep MINIO_NOTIFY
```

### Verify Webhook Configuration
```bash
docker-compose exec mc mc admin config get local/ notify_webhook:airflow
```

### Test Webhook Endpoint
```bash
curl http://localhost:8000/health
```

### Check Event Notifications
```bash
docker-compose exec mc mc event list local/sales
```

### Monitor Logs
```bash
# Webhook logs
docker-compose logs webhook --follow

# MinIO logs
docker-compose logs minio --follow

# Airflow logs
docker-compose logs airflow-webserver --follow
```

## Common Troubleshooting

### Services Not Starting
- Check port conflicts (8080, 9000, 9001, 3000, 5432, 8000)
- Ensure Docker has sufficient resources allocated
- Check docker-compose syntax with `docker-compose config`

### Webhook Not Receiving Events
- Verify MinIO webhook configuration: `mc admin config get local/ notify_webhook:airflow`
- Check network connectivity: `docker-compose exec minio curl http://webhook:8000/`
- Verify event notifications: `mc event list local/sales`

### Airflow DAG Not Triggering
- Ensure DAG is unpaused in Airflow UI
- Check webhook logs for URL decoding issues
- Verify Airflow API connectivity from webhook container
- Check file path matching logic (prefix/suffix requirements)

### Authentication Issues
- Verify auth tokens match between MinIO and webhook configuration
- Check Airflow credentials in webhook environment variables
- Ensure Airflow API auth is properly configured

This documentation provides a complete reference for recreating the MinIO webhook integration with Airflow, including all issues encountered and their specific solutions.