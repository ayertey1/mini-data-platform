# MinIO Webhook Integration with Airflow - Complete Documentation

## Overview
This document details the complete setup process for integrating MinIO object storage with Apache Airflow using webhook notifications. The system automatically triggers Airflow DAGs when CSV files are uploaded to specific MinIO buckets.

## Architecture
- **MinIO**: Object storage service that sends webhook notifications on file uploads
- **Webhook Service**: FastAPI application that receives MinIO events and calls Airflow API
- **Airflow**: Workflow orchestration platform that executes data pipelines
- **PostgreSQL**: Database for Airflow metadata and Metabase
- **Metabase**: Business intelligence tool for data visualization

![Alt text](architecure/image.png)

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

## Complete Working Configuration

### docker-compose.yml

### webhook/app.py

### webhook/requirements.txt

### webhook/Dockerfile


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

## Initial Problem
The original issue was that MinIO environment variables for webhook configuration were not being passed to the MinIO container, preventing webhook notifications from being configured.

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

This documentation provides a complete reference for recreating the MinIO webhook integration with Airflow, including all issues encountered and their specific solutions.