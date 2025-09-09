#!/usr/bin/env python3
"""
Data Quality Monitoring Job - Demonstrates data quality lineage tracking
"""

import logging
import os
import uuid
from datetime import datetime
from typing import Dict, List

from openlineage.client import OpenLineageClient
from openlineage.client.facet import ColumnLineageDatasetFacet, SchemaDatasetFacet
from openlineage.client.run import Job, Run, RunEvent, RunState

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityLineageEmitter:
    """Handles emission of lineage events for data quality monitoring."""

    def __init__(self, marquez_url: str = None, namespace: str = "data-lineage-audit"):
        self.namespace = namespace
        self.marquez_url = marquez_url or os.getenv(
            "MARQUEZ_URL", "http://localhost:5002"
        )

        # Initialize OpenLineage client with HTTP transport
        os.environ["OPENLINEAGE_URL"] = self.marquez_url
        os.environ["OPENLINEAGE_NAMESPACE"] = self.namespace
        os.environ["OPENLINEAGE_ENDPOINT"] = f"{self.marquez_url}/api/v1/lineage"
        self.client = OpenLineageClient()

        logger.info(
            f"Initialized DataQualityLineageEmitter with Marquez URL: {self.marquez_url}"
        )

    def emit_job_start(
        self,
        job_name: str,
        run_id: str,
        inputs: List[Dict] = None,
        outputs: List[Dict] = None,
        job_description: str = None,
    ) -> None:
        """Emit job start event."""

        # Create run
        run = Run(runId=run_id)

        # Create job
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
                    },
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
                    },
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
            producer="https://github.com/OpenLineage/OpenLineage/tree/main/integration/python",
        )

        self.client.emit(event)
        logger.info(f"Emitted job start event for {job_name} (run_id: {run_id})")

    def emit_job_complete(
        self,
        job_name: str,
        run_id: str,
        inputs: List[Dict] = None,
        outputs: List[Dict] = None,
    ) -> None:
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
                    },
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
                    },
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
            producer="https://github.com/OpenLineage/OpenLineage/tree/main/integration/python",
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
            producer="https://github.com/OpenLineage/OpenLineage/tree/main/integration/python",
        )

        self.client.emit(event)
        logger.info(f"Emitted job fail event for {job_name} (run_id: {run_id})")


def main():
    """Main function to run data quality monitoring job."""

    # Initialize lineage emitter
    emitter = DataQualityLineageEmitter()

    # Generate unique run ID
    run_id = str(uuid.uuid4())

    try:
        # Define input datasets
        inputs = [
            {
                "name": "raw_customers",
                "schema": [
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "name", "type": "VARCHAR"},
                    {"name": "email", "type": "VARCHAR"},
                    {"name": "phone", "type": "VARCHAR"},
                    {"name": "address", "type": "VARCHAR"},
                    {"name": "date_of_birth", "type": "DATE"},
                    {"name": "registration_date", "type": "TIMESTAMP"},
                ],
            },
            {
                "name": "raw_orders",
                "schema": [
                    {"name": "order_id", "type": "VARCHAR"},
                    {"name": "customer_id", "type": "VARCHAR"},
                    {"name": "product_id", "type": "VARCHAR"},
                    {"name": "quantity", "type": "INTEGER"},
                    {"name": "order_date", "type": "TIMESTAMP"},
                    {"name": "unit_price", "type": "DECIMAL"},
                    {"name": "status", "type": "VARCHAR"},
                ],
            },
            {
                "name": "raw_transactions",
                "schema": [
                    {"name": "transaction_id", "type": "VARCHAR"},
                    {"name": "account_id", "type": "VARCHAR"},
                    {"name": "amount", "type": "DECIMAL"},
                    {"name": "transaction_date", "type": "TIMESTAMP"},
                    {"name": "status", "type": "VARCHAR"},
                ],
            },
        ]

        # Define output datasets
        outputs = [
            {
                "name": "data_quality_report",
                "schema": [
                    {"name": "dataset_name", "type": "VARCHAR"},
                    {"name": "check_name", "type": "VARCHAR"},
                    {"name": "check_type", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"},
                    {"name": "passed_records", "type": "INTEGER"},
                    {"name": "failed_records", "type": "INTEGER"},
                    {"name": "total_records", "type": "INTEGER"},
                    {"name": "pass_rate", "type": "DECIMAL"},
                    {"name": "check_timestamp", "type": "TIMESTAMP"},
                    {"name": "error_details", "type": "VARCHAR"},
                ],
            },
            {
                "name": "data_lineage_summary",
                "schema": [
                    {"name": "source_dataset", "type": "VARCHAR"},
                    {"name": "target_dataset", "type": "VARCHAR"},
                    {"name": "transformation_type", "type": "VARCHAR"},
                    {"name": "record_count", "type": "INTEGER"},
                    {"name": "quality_score", "type": "DECIMAL"},
                    {"name": "last_updated", "type": "TIMESTAMP"},
                    {"name": "data_freshness", "type": "INTEGER"},
                ],
            },
            {
                "name": "quality_alerts",
                "schema": [
                    {"name": "alert_id", "type": "VARCHAR"},
                    {"name": "dataset_name", "type": "VARCHAR"},
                    {"name": "alert_type", "type": "VARCHAR"},
                    {"name": "severity", "type": "VARCHAR"},
                    {"name": "message", "type": "VARCHAR"},
                    {"name": "threshold_value", "type": "DECIMAL"},
                    {"name": "actual_value", "type": "DECIMAL"},
                    {"name": "alert_timestamp", "type": "TIMESTAMP"},
                    {"name": "status", "type": "VARCHAR"},
                ],
            },
        ]

        # Emit job start
        emitter.emit_job_start(
            job_name="data_quality_monitoring",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            job_description="Monitor data quality across multiple datasets and generate quality reports",
        )

        logger.info("Running data quality checks...")

        # Simulate processing time
        import time

        time.sleep(2)

        # Emit job complete
        emitter.emit_job_complete(
            job_name="data_quality_monitoring",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
        )

        logger.info("Data quality monitoring completed successfully!")

    except Exception as e:
        logger.error(f"Data quality monitoring failed: {str(e)}")
        emitter.emit_job_fail(
            job_name="data_quality_monitoring", run_id=run_id, error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
