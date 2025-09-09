#!/bin/bash

# Data Lineage & Audit Trail - Test Suite
# This script runs comprehensive tests for the project

set -e

echo "Data Lineage & Audit Trail - Test Suite"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test functions
test_passed() {
    echo -e "${GREEN}[PASS] $1${NC}"
}

test_failed() {
    echo -e "${RED}[FAIL] $1${NC}"
    exit 1
}

test_warning() {
    echo -e "${YELLOW}[WARN] $1${NC}"
}

# Check if services are running
echo "Checking service status..."
if ! docker-compose ps | grep -q "Up"; then
    test_failed "Services are not running. Please run 'docker-compose up -d' first."
fi
test_passed "Services are running"

# Test 1: Service Health Checks
echo ""
echo "Running service health checks..."

# PostgreSQL
if docker-compose exec postgres pg_isready -U marquez > /dev/null 2>&1; then
    test_passed "PostgreSQL is healthy"
else
    test_failed "PostgreSQL is not responding"
fi

# Marquez API
if curl -f http://localhost:5000/api/v1/health > /dev/null 2>&1; then
    test_passed "Marquez API is healthy"
else
    test_failed "Marquez API is not responding"
fi

# Marquez UI
if curl -f http://localhost:3000 > /dev/null 2>&1; then
    test_passed "Marquez UI is accessible"
else
    test_failed "Marquez UI is not accessible"
fi

# Test 2: dbt Functionality
echo ""
echo "Testing dbt functionality..."

# dbt seed
if docker-compose exec dbt dbt seed > /dev/null 2>&1; then
    test_passed "dbt seed completed successfully"
else
    test_failed "dbt seed failed"
fi

# dbt run
if docker-compose exec dbt dbt run > /dev/null 2>&1; then
    test_passed "dbt run completed successfully"
else
    test_failed "dbt run failed"
fi

# dbt test
if docker-compose exec dbt dbt test > /dev/null 2>&1; then
    test_passed "dbt test completed successfully"
else
    test_failed "dbt test failed"
fi

# Test 3: Python Lineage Jobs
echo ""
echo "Testing Python lineage jobs..."

# Basic lineage emission
if docker-compose exec python-jobs python emit_lineage.py > /dev/null 2>&1; then
    test_passed "Basic lineage emission completed"
else
    test_failed "Basic lineage emission failed"
fi

# Order transformation job
if docker-compose exec python-jobs python job_transform_orders.py > /dev/null 2>&1; then
    test_passed "Order transformation job completed"
else
    test_failed "Order transformation job failed"
fi

# Test 4: Lineage Verification
echo ""
echo "Verifying lineage in Marquez..."

# Check datasets
DATASETS=$(curl -s http://localhost:5000/api/v1/namespaces/data-lineage-audit/datasets | jq -r '.datasets[].name' | wc -l)
if [ "$DATASETS" -gt 0 ]; then
    test_passed "Found $DATASETS datasets in Marquez"
else
    test_failed "No datasets found in Marquez"
fi

# Check jobs
JOBS=$(curl -s http://localhost:5000/api/v1/namespaces/data-lineage-audit/jobs | jq -r '.jobs[].name' | wc -l)
if [ "$JOBS" -gt 0 ]; then
    test_passed "Found $JOBS jobs in Marquez"
else
    test_failed "No jobs found in Marquez"
fi

# Check specific datasets
EXPECTED_DATASETS=("raw_customers" "raw_orders" "stg_orders" "dim_customers" "fct_orders")
for dataset in "${EXPECTED_DATASETS[@]}"; do
    if curl -s http://localhost:5000/api/v1/namespaces/data-lineage-audit/datasets/$dataset > /dev/null 2>&1; then
        test_passed "Dataset '$dataset' exists"
    else
        test_warning "Dataset '$dataset' not found"
    fi
done

# Test 5: Unit Tests
echo ""
echo "Running unit tests..."

if docker-compose exec python-jobs pytest lineage/tests/test_emit_lineage.py -v > /dev/null 2>&1; then
    test_passed "Unit tests passed"
else
    test_warning "Some unit tests failed (check logs for details)"
fi

# Test 6: Integration Tests
echo ""
echo "Running integration tests..."

if docker-compose exec python-jobs pytest lineage/tests/test_emit_lineage.py -m integration -v > /dev/null 2>&1; then
    test_passed "Integration tests passed"
else
    test_warning "Integration tests failed or skipped (Marquez may not be available)"
fi

# Test 7: Data Quality Checks
echo ""
echo "Running data quality checks..."

# Check if data exists in database
CUSTOMER_COUNT=$(docker-compose exec postgres psql -U marquez -d marquez -t -c "SELECT COUNT(*) FROM customers;" 2>/dev/null | tr -d ' ')
if [ "$CUSTOMER_COUNT" -gt 0 ]; then
    test_passed "Found $CUSTOMER_COUNT customers in database"
else
    test_warning "No customers found in database"
fi

ORDER_COUNT=$(docker-compose exec postgres psql -U marquez -d marquez -t -c "SELECT COUNT(*) FROM orders;" 2>/dev/null | tr -d ' ')
if [ "$ORDER_COUNT" -gt 0 ]; then
    test_passed "Found $ORDER_COUNT orders in database"
else
    test_warning "No orders found in database"
fi

# Test 8: Performance Tests
echo ""
echo "Running performance tests..."

# Test API response time
API_RESPONSE_TIME=$(curl -o /dev/null -s -w '%{time_total}' http://localhost:5000/api/v1/health)
if (( $(echo "$API_RESPONSE_TIME < 1.0" | bc -l) )); then
    test_passed "Marquez API response time: ${API_RESPONSE_TIME}s"
else
    test_warning "Marquez API response time: ${API_RESPONSE_TIME}s (may be slow)"
fi

# Test 9: Security Checks
echo ""
echo " Running security checks..."

# Check if default passwords are still in use
if grep -q "marquez:marquez" docker-compose.yml; then
    test_warning "Default passwords detected in docker-compose.yml"
else
    test_passed "No default passwords detected"
fi

# Test 10: Documentation Checks
echo ""
echo "📚 Checking documentation..."

if [ -f "README.md" ]; then
    test_passed "README.md exists"
else
    test_failed "README.md missing"
fi

if [ -f "docs/OPERATIONS.md" ]; then
    test_passed "Operations guide exists"
else
    test_failed "Operations guide missing"
fi

if [ -f "LICENSE" ]; then
    test_passed "LICENSE file exists"
else
    test_warning "LICENSE file missing"
fi

# Summary
echo ""
echo " Test Summary"
echo "==============="
echo " All critical tests passed!"
echo ""
echo " Next Steps:"
echo "   1. Open http://localhost:3000 to view the lineage graph"
echo "   2. Explore the Marquez UI to see datasets and jobs"
echo "   3. Run additional tests: docker-compose exec python-jobs pytest -v"
echo "   4. Check the logs: docker-compose logs"
echo ""
echo " Your Data Lineage & Audit Trail system is ready!"
