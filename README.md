# Data Lineage & Audit Trail

A comprehensive data lineage tracking system that captures lineage from Python jobs using OpenLineage, stores it in Marquez, and provides a live lineage graph visualization.
<img width="3358" height="1804" alt="image" src="https://github.com/user-attachments/assets/83f26890-c26c-4c39-9e30-b75d2c3da3ab" />
<img width="3424" height="1928" alt="image" src="https://github.com/user-attachments/assets/df3128fc-a4a7-494b-8bb6-125ccbd33a94" />
<img width="3270" height="1826" alt="image" src="https://github.com/user-attachments/assets/f5d85400-f24a-4c05-9363-73e2e3ee7e0e" />
<img width="3428" height="1908" alt="image" src="https://github.com/user-attachments/assets/4236f38c-655b-452d-b96f-bc9ae39414b9" />





## How It Works

This project demonstrates a complete data lineage solution with:

- **Marquez**: OpenLineage backend for storing and querying lineage metadata
- **OpenLineage SDK**: Python job lineage tracking
- **PostgreSQL**: Metadata storage backend
- **Docker Compose**: Easy local development setup

### Data Flow

1. **Python Jobs**: Use OpenLineage SDK to emit lineage events describing data transformations
2. **Marquez**: Stores and indexes all lineage metadata in PostgreSQL
3. **Web UI**: Provides interactive visualization of lineage graphs
4. **API**: Allows programmatic querying of lineage data

### Key Components

- **LineageEmitter**: Python class that handles emission of lineage events
- **Marquez Configuration**: Custom configuration for database connectivity
- **Docker Services**: Orchestrated services for easy deployment

### Available Jobs

The project includes 8 comprehensive example jobs that demonstrate different data lineage scenarios:

1. **Customer Data Processing** (`emit_lineage.py`)
   - Simple ETL job processing customer data
   - Input: raw_customers → Output: processed_customers

2. **Order Processing** (`order_processing.py`)
   - Complex job with multiple inputs and outputs
   - Inputs: raw_orders, customer_master, product_catalog
   - Outputs: enriched_orders, order_summary

3. **Financial Data Processing** (`financial_processing.py`)
   - Financial transactions with currency conversion
   - Inputs: raw_transactions, account_master, exchange_rates
   - Outputs: processed_transactions, daily_account_summary, fraud_indicators

4. **Data Quality Monitoring** (`data_quality_monitoring.py`)
   - Monitors data quality across multiple datasets
   - Inputs: raw_customers, raw_orders, raw_transactions
   - Outputs: data_quality_report, data_lineage_summary, quality_alerts

5. **Real-time Analytics Pipeline** (`real_time_analytics.py`)
   - Streaming data processing for user analytics
   - Inputs: user_events_stream, user_profiles, product_catalog
   - Outputs: real_time_user_analytics, trending_content, personalization_models

6. **Machine Learning Pipeline** (`ml_pipeline.py`)
   - ML model training and prediction pipeline
   - Inputs: training_data, feature_store, model_config
   - Outputs: trained_model, model_predictions, model_metrics, feature_importance

7. **Data Lake Ingestion** (`data_lake_ingestion.py`)
   - Multi-source data ingestion into data lake
   - Inputs: external_api_data, log_files, sensor_data, social_media_feeds
   - Outputs: raw_data_lake, structured_data_lake, data_lake_metadata, data_lineage_tracking

8. **Compliance & Governance** (`compliance_governance.py`)
   - Compliance monitoring and governance tracking
   - Inputs: sensitive_data_inventory, data_access_logs, regulatory_requirements, data_lineage_metadata
   - Outputs: compliance_report, data_privacy_assessment, governance_dashboard, audit_trail

## Quick Start

### Prerequisites

- Docker Desktop installed and running
- Python 3.13+ (for local development)

### Setup

1. **Clone and setup**:
   ```bash
   git clone <your-repo-url>
   cd data-lineage-audit
   cp env.example .env
   ```

2. **Start all services**:
   ```bash
   docker compose up -d
   ```

3. **Wait for services to be ready** (about 30 seconds):
   ```bash
   docker compose ps
   ```

## How to Run

### Start the System

```bash
# Start all services
docker compose up -d

# Check service status
docker compose ps

# View logs if needed
docker compose logs marquez
docker compose logs marquez-web
```

### Run Lineage Jobs

```bash
# Navigate to Python jobs directory
cd lineage/python_jobs

# Activate virtual environment
source venv/bin/activate

# Run individual jobs
python emit_lineage.py                    # Customer data processing
python order_processing.py                # Order processing with multiple inputs
python financial_processing.py            # Financial data with currency conversion
python data_quality_monitoring.py         # Data quality monitoring
python real_time_analytics.py             # Real-time analytics pipeline
python ml_pipeline.py                     # Machine learning pipeline
python data_lake_ingestion.py             # Data lake ingestion
python compliance_governance.py           # Compliance & governance

# Or run all jobs at once
cd ../..
python run_all_jobs.py
```

### Access the Web Interface

- **Marquez UI**: http://localhost:3000
- **Marquez API**: http://localhost:5002
- **Marquez Admin**: http://localhost:5003

## How to Test

### Verify Services Are Running

```bash
# Check all containers are healthy
docker compose ps

# Test Marquez API
curl http://localhost:5002/api/v1/namespaces

# Test Marquez UI
curl http://localhost:3000
```

### Test Lineage Emission

```bash
# Run the lineage job
cd lineage/python_jobs
source venv/bin/activate
python emit_lineage.py

# Verify data was stored
curl http://localhost:5002/api/v1/jobs
curl http://localhost:5002/api/v1/namespaces/data-lineage-audit/datasets
```

### Run Test Suite

```bash
# Run all tests
cd lineage/python_jobs
source venv/bin/activate
pytest

# Run specific test files
pytest lineage/tests/test_emit_lineage.py -v
```

### Explore the Data

1. **Web Interface**: Visit http://localhost:3000 to see interactive lineage graphs
2. **API Queries**: Use curl or your preferred HTTP client to query the Marquez API
3. **Job Details**: View specific job runs and their lineage relationships
4. **Dataset Schemas**: Examine dataset schemas and field-level lineage

## Project Structure

```
data-lineage-audit/
├─ README.md                 # This file
├─ docker-compose.yml       # Service orchestration
├─ marquez-config.yml       # Marquez configuration
├─ env.example              # Environment variables template
├─ lineage/
│  ├─ python_jobs/          # Python lineage jobs
│  │  ├─ emit_lineage.py    # Main lineage emission script
│  │  ├─ requirements.txt   # Python dependencies
│  │  └─ venv/              # Virtual environment
│  └─ tests/                # Test suite
└─ docs/                    # Documentation
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Marquez UI | 3000 | Lineage visualization interface |
| Marquez API | 5002 | OpenLineage backend API |
| Marquez Admin | 5003 | Marquez admin interface |
| PostgreSQL | 5432 | Metadata storage |

## API Endpoints

### Marquez API Examples

```bash
# List all jobs
curl http://localhost:5002/api/v1/jobs

# List datasets in namespace
curl http://localhost:5002/api/v1/namespaces/data-lineage-audit/datasets

# Get specific job details
curl http://localhost:5002/api/v1/namespaces/data-lineage-audit/jobs/customer_data_processing

# Get job runs
curl http://localhost:5002/api/v1/namespaces/data-lineage-audit/jobs/customer_data_processing/runs
```

## Troubleshooting

### Common Issues

**Services won't start:**
```bash
# Check Docker is running
docker --version

# Check service status
docker compose ps

# View logs
docker compose logs marquez
```

**Lineage jobs fail:**
```bash
# Verify Marquez is accessible
curl http://localhost:5002/api/v1/namespaces

# Check Python environment
cd lineage/python_jobs
source venv/bin/activate
python -c "import openlineage.client; print('OpenLineage client available')"
```

**Web UI not accessible:**
```bash
# Check if Marquez UI container is running
docker compose ps marquez-web

# Check UI logs
docker compose logs marquez-web
```

## Documentation

- [Architecture Overview](docs/diagram-architecture.mmd)
- [Data Flow Diagram](docs/diagram-flow.mmd)
- [Operations Guide](docs/OPERATIONS.md)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.
