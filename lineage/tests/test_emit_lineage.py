"""
Test suite for lineage emission functionality.

This module contains comprehensive tests for the OpenLineage SDK integration,
ensuring that lineage events are properly emitted and captured.
"""

import pytest
import os
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from typing import Dict, List

# Import the modules to test
import sys
sys.path.append('/app')
from emit_lineage import LineageEmitter


class TestLineageEmitter:
    """Test cases for LineageEmitter class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.emitter = LineageEmitter(
            marquez_url="http://test-marquez:5000",
            namespace="test-namespace"
        )
        
        # Mock the OpenLineage client
        self.emitter.client = Mock()
    
    def test_initialization(self):
        """Test LineageEmitter initialization."""
        emitter = LineageEmitter(
            marquez_url="http://test-marquez:5000",
            namespace="test-namespace"
        )
        
        assert emitter.namespace == "test-namespace"
        assert emitter.marquez_url == "http://test-marquez:5000"
        assert emitter.client is not None
    
    def test_initialization_with_env_vars(self):
        """Test initialization with environment variables."""
        with patch.dict(os.environ, {
            'MARQUEZ_URL': 'http://env-marquez:5000',
            'OPENLINEAGE_NAMESPACE': 'env-namespace'
        }):
            emitter = LineageEmitter()
            assert emitter.marquez_url == "http://env-marquez:5000"
            assert emitter.namespace == "env-namespace"
    
    def test_create_dataset(self):
        """Test dataset creation with facets."""
        schema = [
            {"name": "id", "type": "integer", "description": "ID field"},
            {"name": "name", "type": "varchar", "description": "Name field"}
        ]
        
        dataset = self.emitter._create_dataset(
            name="test_dataset",
            namespace="test-namespace",
            description="Test dataset",
            schema=schema,
            data_source="postgresql"
        )
        
        assert dataset.name == "test_dataset"
        assert dataset.namespace == "test-namespace"
        assert "schema" in dataset.facets
        assert "dataSource" in dataset.facets
        assert "documentation" in dataset.facets
        assert "ownership" in dataset.facets
    
    def test_emit_job_start(self):
        """Test job start event emission."""
        inputs = [
            {
                "name": "input_dataset",
                "description": "Input dataset",
                "schema": [{"name": "id", "type": "integer"}],
                "data_source": "postgresql"
            }
        ]
        
        outputs = [
            {
                "name": "output_dataset", 
                "description": "Output dataset",
                "schema": [{"name": "id", "type": "integer"}],
                "data_source": "postgresql"
            }
        ]
        
        self.emitter.emit_job_start(
            job_name="test_job",
            run_id="test-run-123",
            inputs=inputs,
            outputs=outputs,
            job_description="Test job"
        )
        
        # Verify that emit was called
        self.emitter.client.emit.assert_called_once()
        
        # Get the emitted event
        emitted_event = self.emitter.client.emit.call_args[0][0]
        
        assert emitted_event.eventType.value == "START"
        assert emitted_event.run.runId == "test-run-123"
        assert emitted_event.job.name == "test_job"
        assert emitted_event.job.namespace == "test-namespace"
        assert len(emitted_event.inputs) == 1
        assert len(emitted_event.outputs) == 1
    
    def test_emit_job_complete(self):
        """Test job complete event emission."""
        inputs = [
            {
                "name": "input_dataset",
                "description": "Input dataset",
                "schema": [{"name": "id", "type": "integer"}],
                "data_source": "postgresql"
            }
        ]
        
        outputs = [
            {
                "name": "output_dataset",
                "description": "Output dataset", 
                "schema": [{"name": "id", "type": "integer"}],
                "data_source": "postgresql"
            }
        ]
        
        self.emitter.emit_job_complete(
            job_name="test_job",
            run_id="test-run-123",
            inputs=inputs,
            outputs=outputs
        )
        
        # Verify that emit was called
        self.emitter.client.emit.assert_called_once()
        
        # Get the emitted event
        emitted_event = self.emitter.client.emit.call_args[0][0]
        
        assert emitted_event.eventType.value == "COMPLETE"
        assert emitted_event.run.runId == "test-run-123"
        assert emitted_event.job.name == "test_job"
    
    def test_emit_job_fail(self):
        """Test job fail event emission."""
        self.emitter.emit_job_fail(
            job_name="test_job",
            run_id="test-run-123",
            error_message="Test error"
        )
        
        # Verify that emit was called
        self.emitter.client.emit.assert_called_once()
        
        # Get the emitted event
        emitted_event = self.emitter.client.emit.call_args[0][0]
        
        assert emitted_event.eventType.value == "FAIL"
        assert emitted_event.run.runId == "test-run-123"
        assert emitted_event.job.name == "test_job"
    
    def test_emit_job_start_without_datasets(self):
        """Test job start emission without input/output datasets."""
        self.emitter.emit_job_start(
            job_name="test_job",
            run_id="test-run-123"
        )
        
        # Verify that emit was called
        self.emitter.client.emit.assert_called_once()
        
        # Get the emitted event
        emitted_event = self.emitter.client.emit.call_args[0][0]
        
        assert emitted_event.eventType.value == "START"
        assert len(emitted_event.inputs) == 0
        assert len(emitted_event.outputs) == 0


class TestOrderTransformJob:
    """Test cases for OrderTransformJob class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock the database engine
        self.mock_engine = Mock()
        
        # Mock pandas read_sql to return test data
        self.test_orders_df = Mock()
        self.test_customers_df = Mock()
        
        with patch('job_transform_orders.pd.read_sql') as mock_read_sql:
            mock_read_sql.side_effect = [self.test_orders_df, self.test_customers_df]
            
            with patch('job_transform_orders.create_engine', return_value=self.mock_engine):
                from job_transform_orders import OrderTransformJob
                self.job = OrderTransformJob()
    
    def test_initialization(self):
        """Test OrderTransformJob initialization."""
        assert self.job.emitter is not None
        assert self.job.engine is not None
    
    @patch('job_transform_orders.pd.read_sql')
    def test_read_orders_data(self, mock_read_sql):
        """Test reading orders data from database."""
        mock_read_sql.return_value = Mock()
        
        result = self.job._read_orders_data()
        
        mock_read_sql.assert_called_once()
        assert result is not None
    
    @patch('job_transform_orders.pd.read_sql')
    def test_read_customers_data(self, mock_read_sql):
        """Test reading customers data from database."""
        mock_read_sql.return_value = Mock()
        
        result = self.job._read_customers_data()
        
        mock_read_sql.assert_called_once()
        assert result is not None
    
    def test_enrich_orders(self):
        """Test order enrichment logic."""
        # Create mock DataFrames
        orders_df = Mock()
        orders_df.merge.return_value = Mock()
        
        customers_df = Mock()
        
        # Mock the merge result
        enriched_df = Mock()
        enriched_df.__getitem__ = Mock(return_value=Mock())
        enriched_df.__setitem__ = Mock()
        
        orders_df.merge.return_value = enriched_df
        
        result = self.job._enrich_orders(orders_df, customers_df)
        
        # Verify merge was called
        orders_df.merge.assert_called_once()
        assert result is not None
    
    def test_create_order_summary(self):
        """Test order summary creation."""
        # Create mock DataFrame with groupby method
        enriched_df = Mock()
        mock_groupby = Mock()
        mock_agg = Mock()
        mock_reset_index = Mock()
        
        enriched_df.groupby.return_value = mock_groupby
        mock_groupby.agg.return_value = mock_agg
        mock_agg.reset_index.return_value = mock_reset_index
        
        result = self.job._create_order_summary(enriched_df)
        
        # Verify groupby was called
        enriched_df.groupby.assert_called_once()
        assert result is not None
    
    def test_write_enriched_orders(self):
        """Test writing enriched orders to database."""
        mock_df = Mock()
        
        self.job._write_enriched_orders(mock_df)
        
        # Verify to_sql was called
        mock_df.to_sql.assert_called_once_with(
            'enriched_orders',
            self.mock_engine,
            if_exists='replace',
            index=False,
            method='multi'
        )
    
    def test_write_order_summary(self):
        """Test writing order summary to database."""
        mock_df = Mock()
        
        self.job._write_order_summary(mock_df)
        
        # Verify to_sql was called
        mock_df.to_sql.assert_called_once_with(
            'order_summary',
            self.mock_engine,
            if_exists='replace',
            index=False,
            method='multi'
        )


class TestIntegration:
    """Integration tests for lineage functionality."""
    
    @pytest.mark.integration
    @pytest.mark.slow
    def test_lineage_emission_integration(self):
        """Test actual lineage emission to Marquez (requires running Marquez)."""
        # This test would require a running Marquez instance
        # Skip if Marquez is not available
        marquez_url = os.getenv("MARQUEZ_URL", "http://localhost:5000")
        
        try:
            import requests
            response = requests.get(f"{marquez_url}/api/v1/health", timeout=5)
            if response.status_code != 200:
                pytest.skip("Marquez not available")
        except:
            pytest.skip("Marquez not available")
        
        # Test actual emission
        emitter = LineageEmitter(marquez_url=marquez_url)
        
        inputs = [
            {
                "name": "test_input",
                "description": "Test input dataset",
                "schema": [{"name": "id", "type": "integer"}],
                "data_source": "postgresql"
            }
        ]
        
        outputs = [
            {
                "name": "test_output",
                "description": "Test output dataset",
                "schema": [{"name": "id", "type": "integer"}],
                "data_source": "postgresql"
            }
        ]
        
        run_id = f"integration-test-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Emit start event
        emitter.emit_job_start(
            job_name="integration_test_job",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs
        )
        
        # Emit complete event
        emitter.emit_job_complete(
            job_name="integration_test_job",
            run_id=run_id,
            inputs=inputs,
            outputs=outputs
        )
        
        # Verify job exists in Marquez
        response = requests.get(f"{marquez_url}/api/v1/namespaces/data-lineage-audit/jobs")
        assert response.status_code == 200
        
        jobs = response.json()
        job_names = [job['name'] for job in jobs.get('jobs', [])]
        assert "integration_test_job" in job_names


if __name__ == "__main__":
    pytest.main([__file__])
