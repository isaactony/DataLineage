"""
OpenLineage SDK example for emitting lineage metadata to Marquez.

This script demonstrates how to use the OpenLineage Python SDK to emit
lineage events for data transformations, showing the flow of data
through different processing steps.
"""

import logging
import os
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from openlineage.client import OpenLineageClient
from openlineage.client.facet import (
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    DataSourceDatasetFacet,
    DocumentationDatasetFacet,
    OwnershipDatasetFacet,
    OwnershipDatasetFacetOwners,
    SchemaDatasetFacet,
    SchemaField,
)
from openlineage.client.run import (
    Dataset,
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.transport.http import HttpTransport

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LineageEmitter:
    """Handles emission of lineage events to Marquez."""

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

        logger.info(f"Initialized LineageEmitter with Marquez URL: {self.marquez_url}")

    def emit_job_start(
        self,
        job_name: str,
        run_id: str,
        inputs: List[Dict] = None,
        outputs: List[Dict] = None,
        job_description: str = None,
    ) -> None:
        """Emit a job start event."""

        # Create input datasets
        input_datasets = []
        if inputs:
            for input_info in inputs:
                dataset = self._create_dataset(
                    name=input_info["name"],
                    namespace=input_info.get("namespace", self.namespace),
                    description=input_info.get("description"),
                    schema=input_info.get("schema"),
                    data_source=input_info.get("data_source"),
                )
                input_datasets.append(
                    InputDataset(
                        namespace=dataset.namespace,
                        name=dataset.name,
                        facets=dataset.facets,
                    )
                )

        # Create output datasets
        output_datasets = []
        if outputs:
            for output_info in outputs:
                dataset = self._create_dataset(
                    name=output_info["name"],
                    namespace=output_info.get("namespace", self.namespace),
                    description=output_info.get("description"),
                    schema=output_info.get("schema"),
                    data_source=output_info.get("data_source"),
                )
                output_datasets.append(
                    OutputDataset(
                        namespace=dataset.namespace,
                        name=dataset.name,
                        facets=dataset.facets,
                    )
                )

        # Create job and run
        job = Job(namespace=self.namespace, name=job_name, facets={})

        run = Run(runId=run_id)

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

        # Emit the event
        self.client.emit(event)
        logger.info(f"Emitted job start event for {job_name} (run_id: {run_id})")

    def emit_job_complete(
        self,
        job_name: str,
        run_id: str,
        inputs: List[Dict] = None,
        outputs: List[Dict] = None,
    ) -> None:
        """Emit a job complete event."""

        # Create input datasets
        input_datasets = []
        if inputs:
            for input_info in inputs:
                dataset = self._create_dataset(
                    name=input_info["name"],
                    namespace=input_info.get("namespace", self.namespace),
                    description=input_info.get("description"),
                    schema=input_info.get("schema"),
                    data_source=input_info.get("data_source"),
                )
                input_datasets.append(
                    InputDataset(
                        namespace=dataset.namespace,
                        name=dataset.name,
                        facets=dataset.facets,
                    )
                )

        # Create output datasets
        output_datasets = []
        if outputs:
            for output_info in outputs:
                dataset = self._create_dataset(
                    name=output_info["name"],
                    namespace=output_info.get("namespace", self.namespace),
                    description=output_info.get("description"),
                    schema=output_info.get("schema"),
                    data_source=output_info.get("data_source"),
                )
                output_datasets.append(
                    OutputDataset(
                        namespace=dataset.namespace,
                        name=dataset.name,
                        facets=dataset.facets,
                    )
                )

        # Create job and run
        job = Job(namespace=self.namespace, name=job_name, facets={})

        run = Run(runId=run_id)

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

        # Emit the event
        self.client.emit(event)
        logger.info(f"Emitted job complete event for {job_name} (run_id: {run_id})")

    def emit_job_fail(self, job_name: str, run_id: str, error_message: str) -> None:
        """Emit a job fail event."""

        job = Job(namespace=self.namespace, name=job_name, facets={})

        run = Run(runId=run_id)

        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=datetime.now().isoformat(),
            run=run,
            job=job,
            producer="https://github.com/OpenLineage/OpenLineage/tree/main/integration/python",
        )

        self.client.emit(event)
        logger.info(
            f"Emitted job fail event for {job_name} (run_id: {run_id}) - Error: {error_message}"
        )

    def _create_dataset(
        self,
        name: str,
        namespace: str,
        description: str = None,
        schema: List[Dict] = None,
        data_source: str = None,
    ) -> Dataset:
        """Create a dataset with facets."""

        facets = {}

        # Add data source facet
        if data_source:
            facets["dataSource"] = DataSourceDatasetFacet(
                name=data_source, uri=f"postgresql://postgres:5432/{namespace}"
            )

        # Add schema facet
        if schema:
            schema_fields = []
            for field in schema:
                schema_fields.append(
                    SchemaField(
                        name=field["name"],
                        type=field["type"],
                        description=field.get("description"),
                    )
                )
            facets["schema"] = SchemaDatasetFacet(fields=schema_fields)

        # Add documentation facet
        if description:
            facets["documentation"] = DocumentationDatasetFacet(description=description)

        # Add ownership facet
        facets["ownership"] = OwnershipDatasetFacet(
            owners=[OwnershipDatasetFacetOwners(name="data-team", type="TEAM")]
        )

        return Dataset(namespace=namespace, name=name, facets=facets)


def main():
    """Main function to demonstrate lineage emission."""

    # Initialize lineage emitter
    emitter = LineageEmitter()

    # Example: Customer data processing job
    run_id = str(uuid.uuid4())

    # Define input datasets
    inputs = [
        {
            "name": "raw_customers",
            "description": "Raw customer data from source system",
            "schema": [
                {
                    "name": "customer_id",
                    "type": "integer",
                    "description": "Unique customer identifier",
                },
                {
                    "name": "first_name",
                    "type": "varchar",
                    "description": "Customer first name",
                },
                {
                    "name": "last_name",
                    "type": "varchar",
                    "description": "Customer last name",
                },
                {
                    "name": "email",
                    "type": "varchar",
                    "description": "Customer email address",
                },
                {
                    "name": "created_at",
                    "type": "timestamp",
                    "description": "Customer creation timestamp",
                },
            ],
            "data_source": "postgresql",
        }
    ]

    # Define output datasets
    outputs = [
        {
            "name": "processed_customers",
            "description": "Processed and enriched customer data",
            "schema": [
                {
                    "name": "customer_id",
                    "type": "integer",
                    "description": "Unique customer identifier",
                },
                {
                    "name": "full_name",
                    "type": "varchar",
                    "description": "Combined first and last name",
                },
                {
                    "name": "email",
                    "type": "varchar",
                    "description": "Customer email address",
                },
                {
                    "name": "customer_segment",
                    "type": "varchar",
                    "description": "Customer segmentation",
                },
                {
                    "name": "processed_at",
                    "type": "timestamp",
                    "description": "Processing timestamp",
                },
            ],
            "data_source": "postgresql",
        }
    ]

    try:
        # Emit job start
        emitter.emit_job_start(
            job_name="customer_data_processing",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
            job_description="Process and enrich customer data",
        )

        # Simulate processing (in real scenario, this would be actual data processing)
        logger.info("Processing customer data...")
        # ... actual processing logic would go here ...

        # Emit job complete
        emitter.emit_job_complete(
            job_name="customer_data_processing",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs,
        )

        logger.info("Customer data processing completed successfully!")

    except Exception as e:
        # Emit job fail
        emitter.emit_job_fail(
            job_name="customer_data_processing", run_id=run_id, error_message=str(e)
        )
        logger.error(f"Customer data processing failed: {e}")
        raise


if __name__ == "__main__":
    main()
