import json
import logging
import os
from typing import Any, Dict
from urllib.parse import unquote

import requests
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

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
    return {"message": "MinIO Webhook Service", "version": "1.0.0", "status": "running"}


@app.get("/health")
async def health_check():
    """Health check endpoint for Docker"""
    try:
        # You could add additional health checks here if needed
        # For example, checking if Airflow is reachable
        return {
            "status": "healthy",
            "service": "minio-webhook",
            "airflow_url": AIRFLOW_API_URL,
            "target_dag": TARGET_DAG_ID,
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


@app.post("/minio-webhook")
async def minio_webhook(request: Request, token: str = Depends(verify_token)):
    """
    Handle MinIO webhook notifications
    Expected payload from MinIO contains information about uploaded files
    """
    try:
        # Get the JSON payload from MinIO
        payload = await request.json()
        logger.info(f"Received MinIO webhook payload: {json.dumps(payload, indent=2)}")

        # Extract relevant information from MinIO payload
        # MinIO webhook payload structure typically includes:
        # - EventName: type of event (e.g., "s3:ObjectCreated:Put")
        # - Key: object path/key
        # - Records: array of event records

        if "Records" in payload:
            for record in payload["Records"]:
                event_name = record.get("eventName", "")
                bucket_name = record.get("s3", {}).get("bucket", {}).get("name", "")
                object_key = record.get("s3", {}).get("object", {}).get("key", "")

                # URL decode the object key (MinIO sends it URL-encoded)
                decoded_key = unquote(object_key)

                logger.info(
                    f"Processing event: {event_name} for {bucket_name}/{object_key}"
                )
                logger.info(f"Decoded key: {decoded_key}")

                # Check if this is a CSV file upload to the raw/ prefix
                if (
                    event_name.startswith("s3:ObjectCreated")
                    and decoded_key.startswith("raw/")
                    and decoded_key.endswith(".csv")
                ):

                    logger.info(
                        "Matching CSV file detected, triggering Airflow DAG..."
                    )

                    # Trigger Airflow DAG
                    dag_run_result = await trigger_airflow_dag(
                        dag_id=TARGET_DAG_ID,
                        conf={
                            "bucket": bucket_name,
                            "key": decoded_key,  # Use decoded key
                            "original_key": object_key,  # Also include original for reference
                            "event_name": event_name,
                            "triggered_by": "minio_webhook",
                        },
                    )

                    logger.info(f"DAG triggered successfully: {dag_run_result}")
                else:
                    logger.info(
                        f"File does not match criteria - Event: {event_name}, Key: {decoded_key}"
                    )

        return {
            "status": "success",
            "message": "Webhook processed successfully",
            "processed_records": len(payload.get("Records", [])),
        }

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Webhook processing failed: {str(e)}"
        )


async def trigger_airflow_dag(dag_id: str, conf: Dict[str, Any] = None):
    """
    Trigger an Airflow DAG via REST API
    """
    try:
        url = f"{AIRFLOW_API_URL}/dags/{dag_id}/dagRuns"

        logger.info(f"Attempting to trigger DAG {dag_id} at {url}")
        logger.info(f"DAG configuration: {conf}")

        # Prepare the payload for Airflow API
        airflow_payload = {"conf": conf or {}}

        # Make request to Airflow API with basic auth
        response = requests.post(
            url,
            json=airflow_payload,
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        logger.info(f"Airflow API response status: {response.status_code}")

        if response.status_code in [200, 201]:
            logger.info(f"Successfully triggered DAG {dag_id}")
            return response.json()
        else:
            logger.error(
                f"Failed to trigger DAG {dag_id}: {response.status_code} - {response.text}"
            )
            raise HTTPException(
                status_code=response.status_code,
                detail=f"Failed to trigger Airflow DAG: {response.text}",
            )

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error when calling Airflow API: {str(e)}")
        raise HTTPException(
            status_code=503, detail=f"Airflow API unreachable: {str(e)}"
        )


@app.get("/test-airflow")
async def test_airflow_connection():
    """
    Test endpoint to verify Airflow connectivity
    """
    try:
        # Test Airflow API connection
        url = f"{AIRFLOW_API_URL}/health"
        response = requests.get(url, auth=(AIRFLOW_USER, AIRFLOW_PASS), timeout=10)

        return {
            "airflow_status": response.status_code,
            "airflow_response": (
                response.json() if response.status_code == 200 else response.text
            ),
            "airflow_url": url,
        }
    except Exception as e:
        return {"error": str(e), "airflow_url": AIRFLOW_API_URL}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(WEBHOOK_PORT))
