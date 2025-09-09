#!/usr/bin/env python3
"""
Machine Learning Pipeline - Demonstrates ML model lineage tracking
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

class MLLineageEmitter:
    """Handles emission of lineage events for ML pipeline processing."""
    
    def __init__(self, marquez_url: str = None, namespace: str = "data-lineage-audit"):
        self.namespace = namespace
        self.marquez_url = marquez_url or os.getenv("MARQUEZ_URL", "http://localhost:5002")
        
        # Initialize OpenLineage client with HTTP transport
        os.environ['OPENLINEAGE_URL'] = self.marquez_url
        os.environ['OPENLINEAGE_NAMESPACE'] = self.namespace
        os.environ['OPENLINEAGE_ENDPOINT'] = f"{self.marquez_url}/api/v1/lineage"
        self.client = OpenLineageClient()
        
        logger.info(f"Initialized MLLineageEmitter with Marquez URL: {self.marquez_url}")
    
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
    """Main function to run ML pipeline."""
    
    # Initialize lineage emitter
    emitter = MLLineageEmitter()
    
    # Generate unique run ID
    run_id = str(uuid.uuid4())
    
    try:
        # Define input datasets
        inputs = [
            {
                "name": "training_data",
                "schema": [
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "age", "type": "INTEGER"},
                    {"name": "income", "type": "DECIMAL"},
                    {"name": "credit_score", "type": "INTEGER"},
                    {"name": "loan_amount", "type": "DECIMAL"},
                    {"name": "loan_term", "type": "INTEGER"},
                    {"name": "employment_years", "type": "INTEGER"},
                    {"name": "debt_to_income", "type": "DECIMAL"},
                    {"name": "default_flag", "type": "BOOLEAN"}
                ]
            },
            {
                "name": "feature_store",
                "schema": [
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "feature_name", "type": "VARCHAR"},
                    {"name": "feature_value", "type": "DECIMAL"},
                    {"name": "feature_type", "type": "VARCHAR"},
                    {"name": "created_at", "type": "TIMESTAMP"},
                    {"name": "version", "type": "VARCHAR"}
                ]
            },
            {
                "name": "model_config",
                "schema": [
                    {"name": "model_name", "type": "VARCHAR"},
                    {"name": "algorithm", "type": "VARCHAR"},
                    {"name": "hyperparameters", "type": "VARCHAR"},
                    {"name": "training_split", "type": "DECIMAL"},
                    {"name": "validation_split", "type": "DECIMAL"},
                    {"name": "test_split", "type": "DECIMAL"},
                    {"name": "config_version", "type": "VARCHAR"}
                ]
            }
        ]
        
        # Define output datasets
        outputs = [
            {
                "name": "trained_model",
                "schema": [
                    {"name": "model_id", "type": "VARCHAR"},
                    {"name": "model_name", "type": "VARCHAR"},
                    {"name": "algorithm", "type": "VARCHAR"},
                    {"name": "model_version", "type": "VARCHAR"},
                    {"name": "training_accuracy", "type": "DECIMAL"},
                    {"name": "validation_accuracy", "type": "DECIMAL"},
                    {"name": "test_accuracy", "type": "DECIMAL"},
                    {"name": "model_path", "type": "VARCHAR"},
                    {"name": "training_timestamp", "type": "TIMESTAMP"}
                ]
            },
            {
                "name": "model_predictions",
                "schema": [
                    {"name": "prediction_id", "type": "VARCHAR"},
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "model_id", "type": "VARCHAR"},
                    {"name": "prediction_score", "type": "DECIMAL"},
                    {"name": "prediction_class", "type": "VARCHAR"},
                    {"name": "confidence", "type": "DECIMAL"},
                    {"name": "prediction_timestamp", "type": "TIMESTAMP"}
                ]
            },
            {
                "name": "model_metrics",
                "schema": [
                    {"name": "model_id", "type": "VARCHAR"},
                    {"name": "metric_name", "type": "VARCHAR"},
                    {"name": "metric_value", "type": "DECIMAL"},
                    {"name": "metric_type", "type": "VARCHAR"},
                    {"name": "dataset_split", "type": "VARCHAR"},
                    {"name": "evaluation_timestamp", "type": "TIMESTAMP"}
                ]
            },
            {
                "name": "feature_importance",
                "schema": [
                    {"name": "model_id", "type": "VARCHAR"},
                    {"name": "feature_name", "type": "VARCHAR"},
                    {"name": "importance_score", "type": "DECIMAL"},
                    {"name": "rank", "type": "INTEGER"},
                    {"name": "feature_type", "type": "VARCHAR"},
                    {"name": "analysis_timestamp", "type": "TIMESTAMP"}
                ]
            }
        ]
        
        # Emit job start
        emitter.emit_job_start(
            job_name="ml_pipeline",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            job_description="Train ML model for credit risk prediction with feature engineering"
        )
        
        logger.info("Training ML model...")
        
        # Simulate processing time
        import time
        time.sleep(3)
        
        # Emit job complete
        emitter.emit_job_complete(
            job_name="ml_pipeline",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs
        )
        
        logger.info("ML pipeline training completed successfully!")
        
    except Exception as e:
        logger.error(f"ML pipeline training failed: {str(e)}")
        emitter.emit_job_fail(
            job_name="ml_pipeline",
            run_id=run_id,
            error_message=str(e)
        )
        raise

if __name__ == "__main__":
    main()
