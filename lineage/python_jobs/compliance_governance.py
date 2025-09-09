#!/usr/bin/env python3
"""
Compliance & Governance Pipeline - Demonstrates compliance data lineage tracking
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


class ComplianceLineageEmitter:
    """Handles emission of lineage events for compliance and governance."""

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
            f"Initialized ComplianceLineageEmitter with Marquez URL: {self.marquez_url}"
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
    """Main function to run compliance and governance pipeline."""

    # Initialize lineage emitter
    emitter = ComplianceLineageEmitter()

    # Generate unique run ID
    run_id = str(uuid.uuid4())

    try:
        # Define input datasets
        inputs = [
            {
                "name": "sensitive_data_inventory",
                "schema": [
                    {"name": "data_element", "type": "VARCHAR"},
                    {"name": "data_classification", "type": "VARCHAR"},
                    {"name": "pii_flag", "type": "BOOLEAN"},
                    {"name": "retention_period", "type": "INTEGER"},
                    {"name": "access_level", "type": "VARCHAR"},
                    {"name": "data_owner", "type": "VARCHAR"},
                    {"name": "last_reviewed", "type": "DATE"},
                ],
            },
            {
                "name": "data_access_logs",
                "schema": [
                    {"name": "user_id", "type": "VARCHAR"},
                    {"name": "dataset_name", "type": "VARCHAR"},
                    {"name": "access_type", "type": "VARCHAR"},
                    {"name": "access_timestamp", "type": "TIMESTAMP"},
                    {"name": "ip_address", "type": "VARCHAR"},
                    {"name": "query_text", "type": "VARCHAR"},
                    {"name": "rows_accessed", "type": "INTEGER"},
                ],
            },
            {
                "name": "regulatory_requirements",
                "schema": [
                    {"name": "regulation_name", "type": "VARCHAR"},
                    {"name": "requirement_id", "type": "VARCHAR"},
                    {"name": "data_type", "type": "VARCHAR"},
                    {"name": "compliance_rule", "type": "VARCHAR"},
                    {"name": "audit_frequency", "type": "VARCHAR"},
                    {"name": "penalty_amount", "type": "DECIMAL"},
                    {"name": "effective_date", "type": "DATE"},
                ],
            },
            {
                "name": "data_lineage_metadata",
                "schema": [
                    {"name": "source_system", "type": "VARCHAR"},
                    {"name": "target_system", "type": "VARCHAR"},
                    {"name": "transformation_type", "type": "VARCHAR"},
                    {"name": "data_flow_path", "type": "VARCHAR"},
                    {"name": "processing_timestamp", "type": "TIMESTAMP"},
                    {"name": "data_volume", "type": "BIGINT"},
                    {"name": "quality_metrics", "type": "JSON"},
                ],
            },
        ]

        # Define output datasets
        outputs = [
            {
                "name": "compliance_report",
                "schema": [
                    {"name": "report_id", "type": "VARCHAR"},
                    {"name": "regulation_name", "type": "VARCHAR"},
                    {"name": "compliance_status", "type": "VARCHAR"},
                    {"name": "violation_count", "type": "INTEGER"},
                    {"name": "risk_score", "type": "DECIMAL"},
                    {"name": "recommendations", "type": "VARCHAR"},
                    {"name": "report_timestamp", "type": "TIMESTAMP"},
                    {"name": "next_audit_date", "type": "DATE"},
                ],
            },
            {
                "name": "data_privacy_assessment",
                "schema": [
                    {"name": "dataset_name", "type": "VARCHAR"},
                    {"name": "privacy_risk_level", "type": "VARCHAR"},
                    {"name": "pii_exposure_score", "type": "DECIMAL"},
                    {"name": "consent_status", "type": "VARCHAR"},
                    {"name": "data_subject_rights", "type": "VARCHAR"},
                    {"name": "retention_compliance", "type": "BOOLEAN"},
                    {"name": "assessment_timestamp", "type": "TIMESTAMP"},
                ],
            },
            {
                "name": "governance_dashboard",
                "schema": [
                    {"name": "metric_name", "type": "VARCHAR"},
                    {"name": "metric_value", "type": "DECIMAL"},
                    {"name": "metric_type", "type": "VARCHAR"},
                    {"name": "threshold_value", "type": "DECIMAL"},
                    {"name": "status", "type": "VARCHAR"},
                    {"name": "trend_direction", "type": "VARCHAR"},
                    {"name": "dashboard_timestamp", "type": "TIMESTAMP"},
                ],
            },
            {
                "name": "audit_trail",
                "schema": [
                    {"name": "audit_id", "type": "VARCHAR"},
                    {"name": "event_type", "type": "VARCHAR"},
                    {"name": "user_id", "type": "VARCHAR"},
                    {"name": "resource_name", "type": "VARCHAR"},
                    {"name": "action_performed", "type": "VARCHAR"},
                    {"name": "event_timestamp", "type": "TIMESTAMP"},
                    {"name": "ip_address", "type": "VARCHAR"},
                    {"name": "success_flag", "type": "BOOLEAN"},
                ],
            },
        ]

        # Emit job start
        emitter.emit_job_start(
            job_name="compliance_governance",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            job_description="Monitor compliance and governance across data systems with audit trails",
        )

        logger.info("Running compliance and governance checks...")

        # Simulate processing time
        import time

        time.sleep(3)

        # Emit job complete
        emitter.emit_job_complete(
            job_name="compliance_governance",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
        )

        logger.info("Compliance and governance processing completed successfully!")

    except Exception as e:
        logger.error(f"Compliance and governance processing failed: {str(e)}")
        emitter.emit_job_fail(
            job_name="compliance_governance", run_id=run_id, error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
