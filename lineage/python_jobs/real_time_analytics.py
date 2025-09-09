#!/usr/bin/env python3
"""
Real-time Analytics Pipeline - Demonstrates streaming data lineage tracking
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


class StreamingLineageEmitter:
    """Handles emission of lineage events for streaming data processing."""

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
            f"Initialized StreamingLineageEmitter with Marquez URL: {self.marquez_url}"
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
    """Main function to run real-time analytics pipeline."""

    # Initialize lineage emitter
    emitter = StreamingLineageEmitter()

    # Generate unique run ID
    run_id = str(uuid.uuid4())

    try:
        # Define input datasets
        inputs = [
            {
                "name": "user_events_stream",
                "schema": [
                    {"name": "event_id", "type": "VARCHAR"},
                    {"name": "user_id", "type": "VARCHAR"},
                    {"name": "session_id", "type": "VARCHAR"},
                    {"name": "event_type", "type": "VARCHAR"},
                    {"name": "page_url", "type": "VARCHAR"},
                    {"name": "timestamp", "type": "TIMESTAMP"},
                    {"name": "device_type", "type": "VARCHAR"},
                    {"name": "browser", "type": "VARCHAR"},
                    {"name": "location", "type": "VARCHAR"},
                    {"name": "referrer", "type": "VARCHAR"},
                ],
            },
            {
                "name": "user_profiles",
                "schema": [
                    {"name": "user_id", "type": "VARCHAR"},
                    {"name": "age_group", "type": "VARCHAR"},
                    {"name": "gender", "type": "VARCHAR"},
                    {"name": "interests", "type": "VARCHAR"},
                    {"name": "subscription_tier", "type": "VARCHAR"},
                    {"name": "registration_date", "type": "DATE"},
                    {"name": "last_login", "type": "TIMESTAMP"},
                ],
            },
            {
                "name": "product_catalog",
                "schema": [
                    {"name": "product_id", "type": "VARCHAR"},
                    {"name": "product_name", "type": "VARCHAR"},
                    {"name": "category", "type": "VARCHAR"},
                    {"name": "price", "type": "DECIMAL"},
                    {"name": "inventory", "type": "INTEGER"},
                    {"name": "tags", "type": "VARCHAR"},
                ],
            },
        ]

        # Define output datasets
        outputs = [
            {
                "name": "real_time_user_analytics",
                "schema": [
                    {"name": "user_id", "type": "VARCHAR"},
                    {"name": "session_id", "type": "VARCHAR"},
                    {"name": "page_views", "type": "INTEGER"},
                    {"name": "session_duration", "type": "INTEGER"},
                    {"name": "bounce_rate", "type": "DECIMAL"},
                    {"name": "conversion_probability", "type": "DECIMAL"},
                    {"name": "recommended_products", "type": "VARCHAR"},
                    {"name": "analytics_timestamp", "type": "TIMESTAMP"},
                ],
            },
            {
                "name": "trending_content",
                "schema": [
                    {"name": "content_id", "type": "VARCHAR"},
                    {"name": "content_type", "type": "VARCHAR"},
                    {"name": "trend_score", "type": "DECIMAL"},
                    {"name": "view_count", "type": "INTEGER"},
                    {"name": "engagement_rate", "type": "DECIMAL"},
                    {"name": "demographic_breakdown", "type": "VARCHAR"},
                    {"name": "trend_timestamp", "type": "TIMESTAMP"},
                ],
            },
            {
                "name": "personalization_models",
                "schema": [
                    {"name": "user_id", "type": "VARCHAR"},
                    {"name": "model_version", "type": "VARCHAR"},
                    {"name": "prediction_confidence", "type": "DECIMAL"},
                    {"name": "recommendation_engine", "type": "VARCHAR"},
                    {"name": "feature_importance", "type": "VARCHAR"},
                    {"name": "model_timestamp", "type": "TIMESTAMP"},
                ],
            },
        ]

        # Emit job start
        emitter.emit_job_start(
            job_name="real_time_analytics",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            job_description="Process real-time user events for analytics and personalization",
        )

        logger.info("Processing real-time user events...")

        # Simulate processing time
        import time

        time.sleep(2)

        # Emit job complete
        emitter.emit_job_complete(
            job_name="real_time_analytics",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
        )

        logger.info("Real-time analytics processing completed successfully!")

    except Exception as e:
        logger.error(f"Real-time analytics processing failed: {str(e)}")
        emitter.emit_job_fail(
            job_name="real_time_analytics", run_id=run_id, error_message=str(e)
        )
        raise


if __name__ == "__main__":
    main()
