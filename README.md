# Data Lineage & Audit Trail

A comprehensive data lineage tracking system that captures lineage from Python jobs using OpenLineage, stores it in Marquez, and provides a live lineage graph visualization.

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

# Run the main lineage job
python emit_lineage.py
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
