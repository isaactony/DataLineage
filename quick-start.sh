#!/bin/bash

# Data Lineage & Audit Trail - Quick Start Script
# This script helps you get started with the project quickly

set -e

echo "🚀 Data Lineage & Audit Trail - Quick Start"
echo "=========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    echo "   Visit: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    echo "   Visit: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "✅ Docker and Docker Compose are installed"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file from template..."
    cp env.example .env
    echo "✅ Created .env file"
else
    echo "✅ .env file already exists"
fi

# Start services
echo "🐳 Starting Docker services..."
docker-compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."

# Wait for PostgreSQL
echo "   Waiting for PostgreSQL..."
timeout 60 bash -c 'until docker-compose exec postgres pg_isready -U marquez; do sleep 2; done'

# Wait for Marquez API
echo "   Waiting for Marquez API..."
timeout 60 bash -c 'until curl -f http://localhost:5000/api/v1/health > /dev/null 2>&1; do sleep 2; done'

# Wait for Marquez UI
echo "   Waiting for Marquez UI..."
timeout 60 bash -c 'until curl -f http://localhost:3000 > /dev/null 2>&1; do sleep 2; done'

echo "✅ All services are ready!"

# Run dbt transformations
echo "🔄 Running dbt transformations..."
echo "   Loading seed data..."
docker-compose exec dbt dbt seed

echo "   Running transformations..."
docker-compose exec dbt dbt run

echo "   Running tests..."
docker-compose exec dbt dbt test

echo "✅ dbt transformations completed!"

# Run Python lineage jobs
echo "🐍 Running Python lineage jobs..."
echo "   Running basic lineage emission..."
docker-compose exec python-jobs python emit_lineage.py

echo "   Running order transformation job..."
docker-compose exec python-jobs python job_transform_orders.py

echo "✅ Python lineage jobs completed!"

# Verify lineage
echo "🔍 Verifying lineage in Marquez..."
echo "   Checking datasets..."
curl -s http://localhost:5000/api/v1/namespaces/data-lineage-audit/datasets | jq -r '.datasets[].name' | head -10

echo "   Checking jobs..."
curl -s http://localhost:5000/api/v1/namespaces/data-lineage-audit/jobs | jq -r '.jobs[].name' | head -10

echo "✅ Lineage verification completed!"

# Display access information
echo ""
echo "🎉 Setup Complete!"
echo "=================="
echo ""
echo "📊 Access Points:"
echo "   • Marquez UI (Lineage Visualization): http://localhost:3000"
echo "   • Marquez API: http://localhost:5000"
echo "   • Airflow UI (Optional): http://localhost:8080"
echo ""
echo "🔧 Useful Commands:"
echo "   • View logs: docker-compose logs [service-name]"
echo "   • Stop services: docker-compose down"
echo "   • Restart services: docker-compose restart"
echo "   • Run tests: docker-compose exec python-jobs pytest"
echo ""
echo "📚 Documentation:"
echo "   • README.md - Project overview and usage"
echo "   • docs/OPERATIONS.md - Operational procedures"
echo "   • docs/diagram-architecture.mmd - System architecture"
echo ""
echo "🎯 Next Steps:"
echo "   1. Open http://localhost:3000 to view the lineage graph"
echo "   2. Explore the datasets and jobs in Marquez UI"
echo "   3. Run additional tests: docker-compose exec python-jobs pytest"
echo "   4. Check out the Airflow DAGs (if enabled)"
echo ""
echo "Happy data lineage tracking! 🚀"
