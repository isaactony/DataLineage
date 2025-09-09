#!/usr/bin/env python3
"""
Financial Data Processing Job - Demonstrates financial data lineage tracking
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

class FinancialLineageEmitter:
    """Handles emission of lineage events for financial data processing."""
    
    def __init__(self, marquez_url: str = None, namespace: str = "data-lineage-audit"):
        self.namespace = namespace
        self.marquez_url = marquez_url or os.getenv("MARQUEZ_URL", "http://localhost:5002")
        
        # Initialize OpenLineage client with HTTP transport
        os.environ['OPENLINEAGE_URL'] = self.marquez_url
        os.environ['OPENLINEAGE_NAMESPACE'] = self.namespace
        os.environ['OPENLINEAGE_ENDPOINT'] = f"{self.marquez_url}/api/v1/lineage"
        self.client = OpenLineageClient()
        
        logger.info(f"Initialized FinancialLineageEmitter with Marquez URL: {self.marquez_url}")
    
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
    """Main function to run financial data processing job."""
    
    # Initialize lineage emitter
    emitter = FinancialLineageEmitter()
    
    # Generate unique run ID
    run_id = str(uuid.uuid4())
    
    try:
        # Define input datasets
        inputs = [
            {
                "name": "raw_transactions",
                "schema": [
                    {"name": "transaction_id", "type": "VARCHAR"},
                    {"name": "account_id", "type": "VARCHAR"},
                    {"name": "transaction_type", "type": "VARCHAR"},
                    {"name": "amount", "type": "DECIMAL"},
                    {"name": "currency", "type": "VARCHAR"},
                    {"name": "transaction_date", "type": "TIMESTAMP"},
                    {"name": "merchant_name", "type": "VARCHAR"},
                    {"name": "category", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"}
                ]
            },
            {
                "name": "account_master",
                "schema": [
                    {"name": "account_id", "type": "VARCHAR"},
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "account_type", "type": "VARCHAR"},
                    {"name": "balance", "type": "DECIMAL"},
                    {"name": "credit_limit", "type": "DECIMAL"},
                    {"name": "opening_date", "type": "DATE"},
                    {"name": "status", "type": "VARCHAR"}
                ]
            },
            {
                "name": "exchange_rates",
                "schema": [
                    {"name": "currency_pair", "type": "VARCHAR"},
                    {"name": "rate", "type": "DECIMAL"},
                    {"name": "rate_date", "type": "DATE"},
                    {"name": "source", "type": "VARCHAR"}
                ]
            }
        ]
        
        # Define output datasets
        outputs = [
            {
                "name": "processed_transactions",
                "schema": [
                    {"name": "transaction_id", "type": "VARCHAR"},
                    {"name": "account_id", "type": "VARCHAR"},
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "account_type", "type": "VARCHAR"},
                    {"name": "transaction_type", "type": "VARCHAR"},
                    {"name": "amount_usd", "type": "DECIMAL"},
                    {"name": "original_amount", "type": "DECIMAL"},
                    {"name": "original_currency", "type": "VARCHAR"},
                    {"name": "exchange_rate", "type": "DECIMAL"},
                    {"name": "transaction_date", "type": "TIMESTAMP"},
                    {"name": "merchant_name", "type": "VARCHAR"},
                    {"name": "category", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"},
                    {"name": "processing_timestamp", "type": "TIMESTAMP"}
                ]
            },
            {
                "name": "daily_account_summary",
                "schema": [
                    {"name": "account_id", "type": "VARCHAR"},
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "summary_date", "type": "DATE"},
                    {"name": "total_debits", "type": "DECIMAL"},
                    {"name": "total_credits", "type": "DECIMAL"},
                    {"name": "net_amount", "type": "DECIMAL"},
                    {"name": "transaction_count", "type": "INTEGER"},
                    {"name": "ending_balance", "type": "DECIMAL"}
                ]
            },
            {
                "name": "fraud_indicators",
                "schema": [
                    {"name": "transaction_id", "type": "VARCHAR"},
                    {"name": "account_id", "type": "VARCHAR"},
                    {"name": "fraud_score", "type": "DECIMAL"},
                    {"name": "risk_factors", "type": "VARCHAR"},
                    {"name": "flag_reason", "type": "VARCHAR"},
                    {"name": "requires_review", "type": "BOOLEAN"},
                    {"name": "analysis_timestamp", "type": "TIMESTAMP"}
                ]
            }
        ]
        
        # Emit job start
        emitter.emit_job_start(
            job_name="financial_data_processing",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            job_description="Process financial transactions with currency conversion and fraud detection"
        )
        
        logger.info("Processing financial data...")
        
        # Simulate processing time
        import time
        time.sleep(3)
        
        # Emit job complete
        emitter.emit_job_complete(
            job_name="financial_data_processing",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs
        )
        
        logger.info("Financial data processing completed successfully!")
        
    except Exception as e:
        logger.error(f"Financial data processing failed: {str(e)}")
        emitter.emit_job_fail(
            job_name="financial_data_processing",
            run_id=run_id,
            error_message=str(e)
        )
        raise

if __name__ == "__main__":
    main()
