#!/usr/bin/env python3
"""
Data Lake Ingestion Pipeline - Demonstrates data lake lineage tracking
"""

import os
import uuid
import logging
from datetime import datetime
from typing import List, Dict
from openlineage.client import OpenLineageClient
from openlineage.client.facet import SchemaDatasetFacet, ColumnLineageDatasetFacet
from openlineage.client.run import RunEvent, RunState, Run, Job

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLakeLineageEmitter:
    """Handles emission of lineage events for data lake ingestion."""
    
    def __init__(self, marquez_url: str = None, namespace: str = "data-lineage-audit"):
        self.namespace = namespace
        self.marquez_url = marquez_url or os.getenv("MARQUEZ_URL", "http://localhost:5002")
        
        # Initialize OpenLineage client with HTTP transport
        os.environ['OPENLINEAGE_URL'] = self.marquez_url
        os.environ['OPENLINEAGE_NAMESPACE'] = self.namespace
        os.environ['OPENLINEAGE_ENDPOINT'] = f"{self.marquez_url}/api/v1/lineage"
        self.client = OpenLineageClient()
        
        logger.info(f"Initialized DataLakeLineageEmitter with Marquez URL: {self.marquez_url}")
    
    def emit_job_start(self, job_name: str, run_id: str, inputs: List[Dict] = None, 
                      outputs: List[Dict] = None, job_description: str = None) -> None:
        """Emit job start event."""
        
        # Create run
        run = Run(runId=run_id)
        
        # Create job
        job = Job(
            namespace=self.namespace,
            name=job_name
        )
        
        # Create input datasets
        input_datasets = []
        if inputs:
            for input_data in inputs:
                dataset = {
                    "namespace": self.namespace,
                    "name": input_data["name"],
                    "facets": {
                        "schema": SchemaDatasetFacet(
                            fields=[
                                {"name": field["name"], "type": field["type"]}
                                for field in input_data.get("schema", [])
                            ]
                        )
                    }
                }
                input_datasets.append(dataset)
        
        # Create output datasets
        output_datasets = []
        if outputs:
            for output_data in outputs:
                dataset = {
                    "namespace": self.namespace,
                    "name": output_data["name"],
                    "facets": {
                        "schema": SchemaDatasetFacet(
                            fields=[
                                {"name": field["name"], "type": field["type"]}
                                for field in output_data.get("schema", [])
                            ]
                        )
                    }
                }
                output_datasets.append(dataset)
        
        # Create run event
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now().isoformat(),
            run=run,
            job=job,
            inputs=input_datasets,
            outputs=output_datasets,
            producer="https://github.com/OpenLineage/OpenLineage/tree/main/integration/python"
        )
        
        self.client.emit(event)
        logger.info(f"Emitted job start event for {job_name} (run_id: {run_id})")
    
    def emit_job_complete(self, job_name: str, run_id: str, inputs: List[Dict] = None, 
                         outputs: List[Dict] = None) -> None:
        """Emit job complete event."""
        
        run = Run(runId=run_id)
        job = Job(namespace=self.namespace, name=job_name)
        
        # Create input datasets
        input_datasets = []
        if inputs:
            for input_data in inputs:
                dataset = {
                    "namespace": self.namespace,
                    "name": input_data["name"],
                    "facets": {
                        "schema": SchemaDatasetFacet(
                            fields=[
                                {"name": field["name"], "type": field["type"]}
                                for field in input_data.get("schema", [])
                            ]
                        )
                    }
                }
                input_datasets.append(dataset)
        
        # Create output datasets
        output_datasets = []
        if outputs:
            for output_data in outputs:
                dataset = {
                    "namespace": self.namespace,
                    "name": output_data["name"],
                    "facets": {
                        "schema": SchemaDatasetFacet(
                            fields=[
                                {"name": field["name"], "type": field["type"]}
                                for field in output_data.get("schema", [])
                            ]
                        )
                    }
                }
                output_datasets.append(dataset)
        
        # Create run event
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=datetime.now().isoformat(),
            run=run,
            job=job,
            inputs=input_datasets,
            outputs=output_datasets,
            producer="https://github.com/OpenLineage/OpenLineage/tree/main/integration/python"
        )
        
        self.client.emit(event)
        logger.info(f"Emitted job complete event for {job_name} (run_id: {run_id})")
    
    def emit_job_fail(self, job_name: str, run_id: str, error_message: str) -> None:
        """Emit job fail event."""
        
        run = Run(runId=run_id)
        job = Job(namespace=self.namespace, name=job_name)
        
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.now().isoformat(),
            run=run,
            job=job,
            producer="https://github.com/OpenLineage/OpenLineage/tree/main/integration/python"
        )
        
        self.client.emit(event)
        logger.info(f"Emitted job fail event for {job_name} (run_id: {run_id})")

def main():
    """Main function to run data lake ingestion pipeline."""
    
    # Initialize lineage emitter
    emitter = DataLakeLineageEmitter()
    
    # Generate unique run ID
    run_id = str(uuid.uuid4())
    
    try:
        # Define input datasets
        inputs = [
            {
                "name": "external_api_data",
                "schema": [
                    {"name": "api_endpoint", "type": "VARCHAR"},
                    {"name": "response_data", "type": "JSON"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "status_code", "type": "INTEGER"},
                    {"name": "response_time", "type": "INTEGER"},
                    {"name": "data_source", "type": "VARCHAR"}
                ]
            },
            {
                "name": "log_files",
                "schema": [
                    {"name": "log_file_path", "type": "VARCHAR"},
                    {"name": "log_level", "type": "VARCHAR"},
                    {"name": "message", "type": "VARCHAR"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "service_name", "type": "VARCHAR"},
                    {"name": "hostname", "type": "VARCHAR"},
                    {"name": "user_id", "type": "VARCHAR"}
                ]
            },
            {
                "name": "sensor_data",
                "schema": [
                    {"name": "sensor_id", "type": "VARCHAR"},
                    {"name": "sensor_type", "type": "VARCHAR"},
                    {"name": "measurement_value", "type": "DECIMAL"},
                    {"name": "unit", "type": "VARCHAR"},
                    {"name": "location", "type": "VARCHAR"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "battery_level", "type": "DECIMAL"}
                ]
            },
            {
                "name": "social_media_feeds",
                "schema": [
                    {"name": "post_id", "type": "VARCHAR"},
                    {"name": "platform", "type": "VARCHAR"},
                    {"name": "user_id", "type": "VARCHAR"},
                    {"name": "content", "type": "VARCHAR"},
                    {"name": "sentiment", "type": "VARCHAR"},
                    {"name": "engagement_metrics", "type": "JSON"},
                    {"name": "timestamp", "type": "TIMESTAMP"}
                ]
            }
        ]
        
        # Define output datasets
        outputs = [
            {
                "name": "raw_data_lake",
                "schema": [
                    {"name": "data_source", "type": "VARCHAR"},
                    {"name": "data_type", "type": "VARCHAR"},
                    {"name": "raw_data", "type": "JSON"},
                    {"name": "ingestion_timestamp", "type": "TIMESTAMP"},
                    {"name": "file_path", "type": "VARCHAR"},
                    {"name": "file_size", "type": "BIGINT"},
                    {"name": "checksum", "type": "VARCHAR"},
                    {"name": "partition_date", "type": "DATE"}
                ]
            },
            {
                "name": "structured_data_lake",
                "schema": [
                    {"name": "table_name", "type": "VARCHAR"},
                    {"name": "schema_version", "type": "VARCHAR"},
                    {"name": "structured_data", "type": "JSON"},
                    {"name": "processing_timestamp", "type": "TIMESTAMP"},
                    {"name": "data_quality_score", "type": "DECIMAL"},
                    {"name": "record_count", "type": "INTEGER"},
                    {"name": "partition_date", "type": "DATE"}
                ]
            },
            {
                "name": "data_lake_metadata",
                "schema": [
                    {"name": "dataset_name", "type": "VARCHAR"},
                    {"name": "data_source", "type": "VARCHAR"},
                    {"name": "schema_info", "type": "JSON"},
                    {"name": "last_updated", "type": "TIMESTAMP"},
                    {"name": "data_freshness", "type": "INTEGER"},
                    {"name": "retention_policy", "type": "VARCHAR"},
                    {"name": "access_permissions", "type": "VARCHAR"}
                ]
            },
            {
                "name": "data_lineage_tracking",
                "schema": [
                    {"name": "source_system", "type": "VARCHAR"},
                    {"name": "target_table", "type": "VARCHAR"},
                    {"name": "transformation_rules", "type": "JSON"},
                    {"name": "data_flow_path", "type": "VARCHAR"},
                    {"name": "processing_time", "type": "INTEGER"},
                    {"name": "success_rate", "type": "DECIMAL"},
                    {"name": "tracking_timestamp", "type": "TIMESTAMP"}
                ]
            }
        ]
        
        # Emit job start
        emitter.emit_job_start(
            job_name="data_lake_ingestion",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            job_description="Ingest data from multiple sources into data lake with schema evolution"
        )
        
        logger.info("Ingesting data into data lake...")
        
        # Simulate processing time
        import time
        time.sleep(4)
        
        # Emit job complete
        emitter.emit_job_complete(
            job_name="data_lake_ingestion",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs
        )
        
        logger.info("Data lake ingestion completed successfully!")
        
    except Exception as e:
        logger.error(f"Data lake ingestion failed: {str(e)}")
        emitter.emit_job_fail(
            job_name="data_lake_ingestion",
            run_id=run_id,
            error_message=str(e)
        )
        raise

if __name__ == "__main__":
    main()
