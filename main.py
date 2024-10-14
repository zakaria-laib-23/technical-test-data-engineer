from data_ingestion_pipeline.data_ingestor_pipeline import DataIngestorPipeline
from etl_data_pipeline.builder import PipelineBuilder

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("combined_pipeline.log"),
    ],
    force=True
)


def run_data_ingestion():
    """Run the data ingestion pipeline."""
    data_types = [
        {
            "endpoint": "/tracks",
            "file_path": "tracks.csv",
            "total_pages": 100  # Total pages known from the API
        },
        {
            "endpoint": "/users",
            "file_path": "users.csv",
            "total_pages": 100  # Total pages known from the API
        },
        {
            "endpoint": "/listen_history",
            "file_path": "listen_history.csv",
            "total_pages": 100  # Total pages known from the API
        }
    ]

    pipeline = DataIngestorPipeline(
        base_url="http://127.0.0.1:8000",
        data_types=data_types
    )

    logging.info("Starting data ingestion pipeline...")
    pipeline.start_pipeline(cpu_per_worker=1)
    pipeline.shutdown_pipeline()
    logging.info("Data ingestion completed.")


def run_etl_pipeline():
    """Run the ETL pipeline after data ingestion."""
    logging.info("Starting ETL pipeline...")

    # Use PipelineBuilder to build and execute the default ETL pipeline
    pipeline = PipelineBuilder().build_default_pipeline()

    # Execute the pipeline
    pipeline.execute()
    logging.info("ETL pipeline completed.")


def main():
    # Run data ingestion first
    run_data_ingestion()

    # After data ingestion is done, run the ETL pipeline
    run_etl_pipeline()


if __name__ == "__main__":
    main()
