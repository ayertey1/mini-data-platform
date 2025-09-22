#!/bin/bash
# Deployment script for Mini Data Platform
# Usage: ./scripts/deploy.sh [dev|prod]

set -e

ENVIRONMENT=${1:-dev}
PROJECT_NAME="mini-data-platform"

echo "Deploying Mini Data Platform to $ENVIRONMENT environment"

# Function to check if service is healthy
check_service_health() {
    local service_name=$1
    local health_url=$2
    local max_attempts=30
    local attempt=1

    echo "Checking $service_name health..."
    while [ $attempt -le $max_attempts ]; do
        if curl -f $health_url > /dev/null 2>&1; then
            echo "$service_name is healthy"
            return 0
        fi
        
        echo "Waiting for $service_name... (attempt $attempt/$max_attempts)"
        sleep 10
        attempt=$((attempt + 1))
    done
    
    echo "$service_name health check failed"
    return 1
}

# Validate environment
if [[ "$ENVIRONMENT" != "dev" && "$ENVIRONMENT" != "prod" ]]; then
    echo "Invalid environment. Use 'dev' or 'prod'"
    exit 1
fi

# Check if required files exist
if [ ! -f "docker-compose.yml" ]; then
    echo "docker-compose.yml not found"
    exit 1
fi

if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        echo "Creating .env from template"
        cp .env.example .env
        echo "Please update .env with your actual values"
    else
        echo "No .env file found and no .env.example template"
        exit 1
    fi
fi

# Environment specific deployment
if [ "$ENVIRONMENT" = "prod" ]; then
    echo "Production deployment"
    
    # Check for production secrets
    if [ -z "$AIRFLOW__FERNET_KEY" ] || [ -z "$AIRFLOW__WEBSERVER__SECRET_KEY" ]; then
        echo "Production secrets not configured"
        echo "Please set AIRFLOW__FERNET_KEY and AIRFLOW__WEBSERVER__SECRET_KEY"
        exit 1
    fi
    
    # Create backup
    echo "Creating backup..."
    timestamp=$(date +%Y%m%d_%H%M%S)
    mkdir -p backups
    
    # Backup databases if they exist
    docker-compose exec -T postgres pg_dump -U ${POSTGRES_USER:-airflow} airflow > "backups/airflow_$timestamp.sql" 2>/dev/null || echo "No existing Airflow DB to backup"
    docker-compose exec -T postgres pg_dump -U ${METABASE_USER:-metabase} ${METABASE_DB:-metabase} > "backups/metabase_$timestamp.sql" 2>/dev/null || echo "No existing Metabase DB to backup"
    
    # Use production compose file
    COMPOSE_FILES="-f docker-compose.yml -f docker-compose.prod.yml"
else
    echo "Development deployment"
    COMPOSE_FILES="-f docker-compose.yml"
fi

# Stop existing services
echo "Stopping existing services..."
docker-compose $COMPOSE_FILES down --remove-orphans

# Pull latest images
echo "Pulling latest images..."
docker-compose $COMPOSE_FILES pull

# Start core infrastructure first
echo "Starting core infrastructure..."
docker-compose $COMPOSE_FILES up -d postgres minio

# Wait for core services
echo "Waiting for core services to be ready..."
sleep 30

# Check PostgreSQL
if ! docker-compose exec -T postgres pg_isready -U ${POSTGRES_USER:-airflow} > /dev/null 2>&1; then
    echo "PostgreSQL is not ready"
    exit 1
fi
echo "PostgreSQL is ready"

# Check MinIO
if ! check_service_health "MinIO" "http://localhost:9000/minio/health/ready"; then
    exit 1
fi

# Start MinIO client to configure webhooks
echo "Configuring MinIO..."
docker-compose $COMPOSE_FILES up -d mc
sleep 20

# Start application services
echo "Starting application services..."
docker-compose $COMPOSE_FILES up -d airflow-webserver airflow-scheduler webhook

# Wait for Airflow to be ready
echo "Waiting for Airflow to be ready..."
sleep 60

if ! check_service_health "Airflow" "http://localhost:8080/health"; then
    echo "Airflow may not be fully ready, but continuing..."
fi

if ! check_service_health "Webhook" "http://localhost:8000/health"; then
    exit 1
fi

# Start remaining services
echo "Starting remaining services..."
docker-compose $COMPOSE_FILES up -d

# Final health check
echo "Running final health checks..."

services_status="All services status:\n"
services_status+="$(docker-compose $COMPOSE_FILES ps)\n"

echo -e "$services_status"

# Test webhook endpoint
echo "Testing webhook endpoint..."
if curl -X POST http://localhost:8000/minio-webhook \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer supersecret" \
     -d '{"test": "deployment_validation"}' > /dev/null 2>&1; then
    echo "Webhook endpoint is responding"
else
    echo "Webhook endpoint test failed"
fi

# Display service URLs
echo ""
echo "Service URLs:"
echo "  MinIO Console: http://localhost:9001"
echo "  Airflow UI: http://localhost:8080"
echo "  Webhook API: http://localhost:8000"
echo "  Metabase: http://localhost:3000"
echo ""

if [ "$ENVIRONMENT" = "dev" ]; then
    echo "To test the pipeline:"
    echo "  python data-generator/generate_sales.py"
    echo ""
fi

echo "Deployment completed successfully!"

# Show logs for monitoring
echo "Recent logs:"
docker-compose $COMPOSE_FILES logs --tail=5 webhook airflow-scheduler