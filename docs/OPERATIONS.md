# Operations Guide - Data Lineage & Audit Trail

## Overview

This guide provides operational procedures for managing and maintaining the Data Lineage & Audit Trail system.

## System Components

### Core Services
- **Marquez**: OpenLineage backend for lineage storage and API
- **PostgreSQL**: Metadata storage database
- **dbt**: SQL transformation engine with automatic lineage capture
- **Python Jobs**: Custom data processing with OpenLineage SDK
- **Airflow**: Workflow orchestration (optional)

### Service Ports
| Service | Port | Purpose |
|---------|------|---------|
| Marquez UI | 3000 | Lineage visualization |
| Marquez API | 5000 | OpenLineage backend |
| PostgreSQL | 5432 | Database access |
| Airflow UI | 8080 | Workflow management |

## Startup Procedures

### 1. Initial Setup
```bash
# Clone repository
git clone <repository-url>
cd data-lineage-audit

# Copy environment configuration
cp env.example .env

# Start all services
docker-compose up -d
```

### 2. Verify Services
```bash
# Check service health
docker-compose ps

# Verify Marquez API
curl http://localhost:5000/api/v1/health

# Verify Marquez UI
curl http://localhost:3000

# Verify PostgreSQL
docker-compose exec postgres pg_isready -U marquez
```

### 3. Initialize Data
```bash
# Load seed data
docker-compose exec dbt dbt seed

# Run transformations
docker-compose exec dbt dbt run

# Run tests
docker-compose exec dbt dbt test
```

## Daily Operations

### 1. Data Pipeline Execution
```bash
# Run complete pipeline
docker-compose exec dbt dbt run
docker-compose exec python-jobs python job_transform_orders.py

# Or use Airflow (if enabled)
docker-compose exec airflow airflow dags trigger dbt_daily_lineage
```

### 2. Data Quality Checks
```bash
# Run dbt tests
docker-compose exec dbt dbt test

# Check lineage completeness
curl http://localhost:5000/api/v1/namespaces/data-lineage-audit/datasets
curl http://localhost:5000/api/v1/namespaces/data-lineage-audit/jobs
```

### 3. Monitoring
```bash
# Check service logs
docker-compose logs marquez
docker-compose logs postgres
docker-compose logs dbt

# Monitor resource usage
docker stats
```

## Troubleshooting

### Common Issues

#### 1. Marquez API Not Responding
```bash
# Check Marquez logs
docker-compose logs marquez

# Restart Marquez
docker-compose restart marquez

# Check database connection
docker-compose exec marquez curl http://localhost:5000/api/v1/health
```

#### 2. Database Connection Issues
```bash
# Check PostgreSQL status
docker-compose exec postgres pg_isready -U marquez

# Check database logs
docker-compose logs postgres

# Restart PostgreSQL
docker-compose restart postgres
```

#### 3. dbt Transformations Failing
```bash
# Check dbt logs
docker-compose logs dbt

# Run dbt debug
docker-compose exec dbt dbt debug

# Check database connectivity
docker-compose exec dbt dbt debug --profiles-dir /app
```

#### 4. Python Jobs Failing
```bash
# Check Python job logs
docker-compose logs python-jobs

# Run jobs manually
docker-compose exec python-jobs python emit_lineage.py
docker-compose exec python-jobs python job_transform_orders.py
```

### Performance Issues

#### 1. Slow Query Performance
```bash
# Check PostgreSQL performance
docker-compose exec postgres psql -U marquez -d marquez -c "SELECT * FROM pg_stat_activity;"

# Analyze query performance
docker-compose exec postgres psql -U marquez -d marquez -c "SELECT query, mean_time FROM pg_stat_statements ORDER BY mean_time DESC LIMIT 10;"
```

#### 2. High Memory Usage
```bash
# Check container resource usage
docker stats

# Restart services if needed
docker-compose restart
```

## Maintenance Procedures

### 1. Database Maintenance
```bash
# Backup database
docker-compose exec postgres pg_dump -U marquez marquez > backup_$(date +%Y%m%d).sql

# Restore database
docker-compose exec -T postgres psql -U marquez marquez < backup_20231201.sql

# Vacuum database
docker-compose exec postgres psql -U marquez -d marquez -c "VACUUM ANALYZE;"
```

### 2. Log Management
```bash
# Clean old logs
docker-compose logs --tail=1000 > recent_logs.txt

# Rotate logs
docker-compose down
docker-compose up -d
```

### 3. Data Cleanup
```bash
# Clean old lineage data (if needed)
docker-compose exec postgres psql -U marquez -d marquez -c "DELETE FROM lineage_events WHERE created_at < NOW() - INTERVAL '30 days';"

# Clean Docker volumes
docker-compose down -v
docker system prune -f
```

## Security Considerations

### 1. Access Control
- Change default passwords in production
- Use environment variables for sensitive data
- Implement proper network security
- Regular security updates

### 2. Data Protection
- Encrypt sensitive data at rest
- Use secure connections (HTTPS/TLS)
- Implement proper backup procedures
- Monitor access logs

### 3. Compliance
- Maintain audit trails
- Document data processing activities
- Implement data retention policies
- Regular compliance reviews

## Scaling Considerations

### 1. Horizontal Scaling
- Use multiple Marquez instances behind a load balancer
- Implement database read replicas
- Use container orchestration (Kubernetes)

### 2. Vertical Scaling
- Increase container resource limits
- Optimize database configuration
- Use faster storage (SSD)

### 3. Performance Optimization
- Implement caching strategies
- Optimize database queries
- Use connection pooling
- Monitor and tune performance

## Backup and Recovery

### 1. Backup Strategy
```bash
# Daily database backup
docker-compose exec postgres pg_dump -U marquez marquez > daily_backup_$(date +%Y%m%d).sql

# Configuration backup
tar -czf config_backup_$(date +%Y%m%d).tar.gz docker-compose.yml .env dbt_project/ lineage/

# Store backups securely
# (Implement your preferred backup storage solution)
```

### 2. Recovery Procedures
```bash
# Restore database
docker-compose exec -T postgres psql -U marquez marquez < backup_file.sql

# Restore configuration
tar -xzf config_backup.tar.gz

# Restart services
docker-compose up -d
```

## Monitoring and Alerting

### 1. Health Checks
```bash
# Create health check script
#!/bin/bash
curl -f http://localhost:5000/api/v1/health || exit 1
curl -f http://localhost:3000 || exit 1
docker-compose ps | grep -v "Up" && exit 1
```

### 2. Metrics Collection
- Monitor CPU, memory, and disk usage
- Track API response times
- Monitor database performance
- Set up alerts for failures

### 3. Log Analysis
- Centralize logs using ELK stack or similar
- Set up log-based alerts
- Regular log analysis for patterns

## Support and Escalation

### 1. Support Contacts
- Data Engineering Team: data-team@company.com
- Infrastructure Team: infra-team@company.com
- On-call Engineer: +1-XXX-XXX-XXXX

### 2. Escalation Procedures
1. Check service health and logs
2. Attempt standard troubleshooting
3. Escalate to team lead if unresolved
4. Escalate to infrastructure team for system issues
5. Document all actions taken

### 3. Documentation Updates
- Update this guide when procedures change
- Document new issues and solutions
- Maintain runbook for common scenarios
- Regular review and updates
