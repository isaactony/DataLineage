#!/usr/bin/env python3
"""
Order Processing Job - Demonstrates complex data lineage with multiple inputs/outputs
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

class OrderLineageEmitter:
    """Handles emission of lineage events for order processing."""
    
    def __init__(self, marquez_url: str = None, namespace: str = "data-lineage-audit"):
        self.namespace = namespace
        self.marquez_url = marquez_url or os.getenv("MARQUEZ_URL", "http://localhost:5002")
        
        # Initialize OpenLineage client with HTTP transport
        os.environ['OPENLINEAGE_URL'] = self.marquez_url
        os.environ['OPENLINEAGE_NAMESPACE'] = self.namespace
        os.environ['OPENLINEAGE_ENDPOINT'] = f"{self.marquez_url}/api/v1/lineage"
        self.client = OpenLineageClient()
        
        logger.info(f"Initialized OrderLineageEmitter with Marquez URL: {self.marquez_url}")
    
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
    """Main function to run order processing job."""
    
    # Initialize lineage emitter
    emitter = OrderLineageEmitter()
    
    # Generate unique run ID
    run_id = str(uuid.uuid4())
    
    try:
        # Define input datasets
        inputs = [
            {
                "name": "raw_orders",
                "schema": [
                    {"name": "order_id", "type": "VARCHAR"},
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "product_id", "type": "VARCHAR"},
                    {"name": "quantity", "type": "INTEGER"},
                    {"name": "order_date", "type": "TIMESTAMP"},
                    {"name": "unit_price", "type": "DECIMAL"},
                    {"name": "status", "type": "VARCHAR"}
                ]
            },
            {
                "name": "customer_master",
                "schema": [
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "customer_name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR"},
                    {"name": "phone", "type": "VARCHAR"},
                    {"name": "address", "type": "VARCHAR"},
                    {"name": "customer_tier", "type": "VARCHAR"}
                ]
            },
            {
                "name": "product_catalog",
                "schema": [
                    {"name": "product_id", "type": "VARCHAR"},
                    {"name": "product_name", "type": "VARCHAR"},
                    {"name": "category", "type": "VARCHAR"},
                    {"name": "brand", "type": "VARCHAR"},
                    {"name": "price", "type": "DECIMAL"},
                    {"name": "in_stock", "type": "BOOLEAN"}
                ]
            }
        ]
        
        # Define output datasets
        outputs = [
            {
                "name": "enriched_orders",
                "schema": [
                    {"name": "order_id", "type": "VARCHAR"},
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "customer_name", "type": "VARCHAR"},
                    {"name": "customer_tier", "type": "VARCHAR"},
                    {"name": "product_id", "type": "VARCHAR"},
                    {"name": "product_name", "type": "VARCHAR"},
                    {"name": "category", "type": "VARCHAR"},
                    {"name": "brand", "type": "VARCHAR"},
                    {"name": "quantity", "type": "INTEGER"},
                    {"name": "unit_price", "type": "DECIMAL"},
                    {"name": "total_amount", "type": "DECIMAL"},
                    {"name": "order_date", "type": "TIMESTAMP"},
                    {"name": "status", "type": "VARCHAR"},
                    {"name": "processing_timestamp", "type": "TIMESTAMP"}
                ]
            },
            {
                "name": "order_summary",
                "schema": [
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "total_orders", "type": "INTEGER"},
                    {"name": "total_amount", "type": "DECIMAL"},
                    {"name": "avg_order_value", "type": "DECIMAL"},
                    {"name": "last_order_date", "type": "TIMESTAMP"},
                    {"name": "customer_tier", "type": "VARCHAR"}
                ]
            }
        ]
        
        # Emit job start
        emitter.emit_job_start(
            job_name="order_processing",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            job_description="Process and enrich order data with customer and product information"
        )
        
        logger.info("Processing order data...")
        
        # Simulate processing time
        import time
        time.sleep(2)
        
        # Emit job complete
        emitter.emit_job_complete(
            job_name="order_processing",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs
        )
        
        logger.info("Order processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Order processing failed: {str(e)}")
        emitter.emit_job_fail(
            job_name="order_processing",
            run_id=run_id,
            error_message=str(e)
        )
        raise

if __name__ == "__main__":
    main()
