"""
Example job that transforms order data and emits lineage information.

This script demonstrates a more complex data transformation job that:
1. Reads order data from multiple sources
2. Performs data transformations
3. Writes results to output tables
4. Emits comprehensive lineage information throughout the process
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from emit_lineage import LineageEmitter

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OrderTransformJob:
    """Handles order data transformation with lineage tracking."""

    def __init__(self, marquez_url: str = None):
        self.emitter = LineageEmitter(marquez_url)
        self.db_url = os.getenv(
            "DATABASE_URL", "postgresql://marquez:marquez@postgres:5432/marquez"
        )
        self.engine = create_engine(self.db_url)

    def run(self):
        """Run the order transformation job."""

        run_id = f"order-transform-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

        # Define input datasets
        inputs = [
            {
                "name": "raw_orders",
                "description": "Raw order data from source system",
                "schema": [
                    {
                        "name": "order_id",
                        "type": "integer",
                        "description": "Unique order identifier",
                    },
                    {
                        "name": "customer_id",
                        "type": "integer",
                        "description": "Customer identifier",
                    },
                    {"name": "order_date", "type": "date", "description": "Order date"},
                    {
                        "name": "amount",
                        "type": "decimal",
                        "description": "Order amount",
                    },
                    {
                        "name": "status",
                        "type": "varchar",
                        "description": "Order status",
                    },
                ],
                "data_source": "postgresql",
            },
            {
                "name": "raw_customers",
                "description": "Customer master data",
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
                ],
                "data_source": "postgresql",
            },
        ]

        # Define output datasets
        outputs = [
            {
                "name": "enriched_orders",
                "description": "Orders enriched with customer information",
                "schema": [
                    {
                        "name": "order_id",
                        "type": "integer",
                        "description": "Unique order identifier",
                    },
                    {
                        "name": "customer_id",
                        "type": "integer",
                        "description": "Customer identifier",
                    },
                    {
                        "name": "customer_name",
                        "type": "varchar",
                        "description": "Full customer name",
                    },
                    {
                        "name": "customer_email",
                        "type": "varchar",
                        "description": "Customer email",
                    },
                    {"name": "order_date", "type": "date", "description": "Order date"},
                    {
                        "name": "amount",
                        "type": "decimal",
                        "description": "Order amount",
                    },
                    {
                        "name": "status",
                        "type": "varchar",
                        "description": "Order status",
                    },
                    {
                        "name": "order_month",
                        "type": "varchar",
                        "description": "Order month",
                    },
                    {
                        "name": "processed_at",
                        "type": "timestamp",
                        "description": "Processing timestamp",
                    },
                ],
                "data_source": "postgresql",
            },
            {
                "name": "order_summary",
                "description": "Aggregated order summary by customer",
                "schema": [
                    {
                        "name": "customer_id",
                        "type": "integer",
                        "description": "Customer identifier",
                    },
                    {
                        "name": "customer_name",
                        "type": "varchar",
                        "description": "Full customer name",
                    },
                    {
                        "name": "total_orders",
                        "type": "integer",
                        "description": "Total number of orders",
                    },
                    {
                        "name": "total_amount",
                        "type": "decimal",
                        "description": "Total order amount",
                    },
                    {
                        "name": "avg_order_value",
                        "type": "decimal",
                        "description": "Average order value",
                    },
                    {
                        "name": "last_order_date",
                        "type": "date",
                        "description": "Last order date",
                    },
                    {
                        "name": "processed_at",
                        "type": "timestamp",
                        "description": "Processing timestamp",
                    },
                ],
                "data_source": "postgresql",
            },
        ]

        try:
            # Emit job start
            self.emitter.emit_job_start(
                job_name="order_data_transformation",
                run_id=run_id,
                inputs=inputs,
                outputs=outputs,
                job_description="Transform and enrich order data with customer information",
            )

            # Step 1: Read raw data
            logger.info("Reading raw order data...")
            orders_df = self._read_orders_data()

            logger.info("Reading customer data...")
            customers_df = self._read_customers_data()

            # Step 2: Transform data
            logger.info("Transforming order data...")
            enriched_orders_df = self._enrich_orders(orders_df, customers_df)

            logger.info("Creating order summary...")
            order_summary_df = self._create_order_summary(enriched_orders_df)

            # Step 3: Write results
            logger.info("Writing enriched orders to database...")
            self._write_enriched_orders(enriched_orders_df)

            logger.info("Writing order summary to database...")
            self._write_order_summary(order_summary_df)

            # Emit job complete
            self.emitter.emit_job_complete(
                job_name="order_data_transformation",
                run_id=run_id,
                inputs=inputs,
                outputs=outputs,
            )

            logger.info("Order transformation job completed successfully!")

        except Exception as e:
            # Emit job fail
            self.emitter.emit_job_fail(
                job_name="order_data_transformation",
                run_id=run_id,
                error_message=str(e),
            )
            logger.error(f"Order transformation job failed: {e}")
            raise

    def _read_orders_data(self) -> pd.DataFrame:
        """Read raw orders data from database."""
        query = """
        SELECT order_id, customer_id, order_date, amount, status
        FROM orders
        ORDER BY order_date DESC
        """
        return pd.read_sql(query, self.engine)

    def _read_customers_data(self) -> pd.DataFrame:
        """Read customer data from database."""
        query = """
        SELECT customer_id, first_name, last_name, email
        FROM customers
        """
        return pd.read_sql(query, self.engine)

    def _enrich_orders(
        self, orders_df: pd.DataFrame, customers_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Enrich orders with customer information."""

        # Merge orders with customer data
        enriched_df = orders_df.merge(customers_df, on="customer_id", how="left")

        # Add derived fields
        enriched_df["customer_name"] = (
            enriched_df["first_name"] + " " + enriched_df["last_name"]
        )
        enriched_df["customer_email"] = enriched_df["email"]
        enriched_df["order_month"] = (
            pd.to_datetime(enriched_df["order_date"]).dt.to_period("M").astype(str)
        )
        enriched_df["processed_at"] = datetime.now()

        # Select final columns
        result_df = enriched_df[
            [
                "order_id",
                "customer_id",
                "customer_name",
                "customer_email",
                "order_date",
                "amount",
                "status",
                "order_month",
                "processed_at",
            ]
        ]

        return result_df

    def _create_order_summary(self, enriched_orders_df: pd.DataFrame) -> pd.DataFrame:
        """Create order summary by customer."""

        summary_df = (
            enriched_orders_df.groupby(["customer_id", "customer_name"])
            .agg({"order_id": "count", "amount": ["sum", "mean"], "order_date": "max"})
            .reset_index()
        )

        # Flatten column names
        summary_df.columns = [
            "customer_id",
            "customer_name",
            "total_orders",
            "total_amount",
            "avg_order_value",
            "last_order_date",
        ]

        # Add processing timestamp
        summary_df["processed_at"] = datetime.now()

        return summary_df

    def _write_enriched_orders(self, df: pd.DataFrame):
        """Write enriched orders to database."""
        df.to_sql(
            "enriched_orders",
            self.engine,
            if_exists="replace",
            index=False,
            method="multi",
        )

    def _write_order_summary(self, df: pd.DataFrame):
        """Write order summary to database."""
        df.to_sql(
            "order_summary",
            self.engine,
            if_exists="replace",
            index=False,
            method="multi",
        )


def main():
    """Main function to run the order transformation job."""

    job = OrderTransformJob()
    job.run()


if __name__ == "__main__":
    main()
