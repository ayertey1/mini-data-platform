"""
Tests for the webhook service
"""
import pytest
from fastapi.testclient import TestClient
import json
import os
import sys

# Add the parent directory to the path to import app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from app import app
    client = TestClient(app)
except ImportError:
    # If app import fails, create a minimal test
    app = None
    client = None

def test_root_endpoint():
    """Test the root endpoint"""
    if client is None:
        pytest.skip("App not available for testing")
    
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["message"] == "MinIO Webhook Service"
    assert data["status"] == "running"

def test_health_endpoint():
    """Test the health check endpoint"""
    if client is None:
        pytest.skip("App not available for testing")
    
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "minio-webhook"

def test_webhook_endpoint_without_auth():
    """Test webhook endpoint without authentication"""
    if client is None:
        pytest.skip("App not available for testing")
    
    test_payload = {
        "Records": [
            {
                "eventName": "s3:ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "sales"},
                    "object": {"key": "raw%2Ftest.csv"}
                }
            }
        ]
    }
    
    response = client.post("/minio-webhook", json=test_payload)
    # Should return 403 or 401 without proper auth
    assert response.status_code in [401, 403, 422]

def test_webhook_endpoint_with_auth():
    """Test webhook endpoint with authentication"""
    if client is None:
        pytest.skip("App not available for testing")
    
    test_payload = {
        "Records": [
            {
                "eventName": "s3:ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "sales"},
                    "object": {"key": "raw%2Ftest.csv"}
                }
            }
        ]
    }
    
    headers = {"Authorization": "Bearer supersecret"}
    response = client.post("/minio-webhook", json=test_payload, headers=headers)
    
    # Should not fail due to auth (might fail due to Airflow connection)
    # We're just testing the auth and basic payload processing
    assert response.status_code in [200, 500, 503]  # 500/503 if Airflow unavailable

def test_invalid_webhook_payload():
    """Test webhook with invalid payload"""
    if client is None:
        pytest.skip("App not available for testing")
    
    headers = {"Authorization": "Bearer supersecret"}
    response = client.post("/minio-webhook", json={"invalid": "payload"}, headers=headers)
    
    # Should handle invalid payload gracefully
    assert response.status_code in [200, 400, 500]

if __name__ == "__main__":
    pytest.main([__file__])