import ray
import logging
from data_ingestion_pipeline.data_ingestor_worker import DataIngestorWorker


class DataIngestorPipeline:
    """
    DataIngestorPipeline encapsulates the full functionality for distributed data ingestion and saving to CSV.
    It dynamically adjusts the number of workers based on the available CPUs in the Ray cluster and supports
    multiple data types through configuration-based extension.
    """

    def __init__(self, base_url: str, data_types: list, log_file: str = 'pipeline.log'):
        """
        Initialize the pipeline with the base URL and a list of data types.

        Args:
            base_url: Base URL for the API.
            data_types: List of dictionaries where each dictionary defines a data type with:
                        - 'endpoint': API endpoint for the data type.
                        - 'file_path': Path to the CSV file where data will be saved.
                        - 'total_pages': Total number of pages for this data type.
            log_file: Path to the log file.
        """
        self.base_url = base_url
        self.data_types = data_types  # List of data types with endpoint, file path, total pages

        # Set up logging to both file and console
        self.setup_logging(log_file)

    def setup_logging(self, log_file: str):
        """
        Set up logging to log to both a file and the console.
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # Create file handler to log to a file
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # Create console handler to log to the console (stdout)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Set formatter for both handlers
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add both handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        logger.info('Logging is set up. Logging to file and console.')

    def get_available_cpus(self):
        """
        Get the total number of available CPUs in the Ray cluster.
        """
        available_resources = ray.available_resources()
        return int(available_resources.get("CPU", 0))

    def distribute_pages_across_workers(self, total_pages, num_workers):
        """
        Distribute pages across workers based on the total number of pages and number of workers.

        Args:
            total_pages: The total number of pages to ingest.
            num_workers: The number of workers available for the task.

        Returns:
            List of page ranges for each worker.
        """
        pages_per_worker = total_pages // num_workers  # Pages each worker should handle

        # Assign ranges of pages to each worker
        page_ranges = [(i * pages_per_worker + 1, (i + 1) * pages_per_worker) for i in range(num_workers)]

        # Last worker may need to process extra pages if total_pages isn't divisible by num_workers
        if total_pages % num_workers != 0:
            page_ranges[-1] = (page_ranges[-1][0], total_pages)

        return page_ranges

    def start_pipeline(self, cpu_per_worker: int = 1):
        """
        Start the ingestion pipeline using Ray to scale horizontally and save data to CSV files.
        It supports multiple data types defined in the data_types attribute.

        Args:
            cpu_per_worker: Number of CPUs allocated to each worker.
        """
        # Initialize Ray
        ray.init(ignore_reinit_error=True)

        # Get available CPUs and calculate the number of workers
        total_cpus = self.get_available_cpus()
        num_workers = total_cpus // cpu_per_worker

        if num_workers == 0:
            logging.error("Not enough CPU resources to allocate workers.")
            return

        logging.info(f"Using {num_workers} workers, each with {cpu_per_worker} CPUs.")

        # Create a list of endpoints (from data_types)
        endpoints = [data_type['endpoint'] for data_type in self.data_types]
        file_base_path = 'data_ingestion_output'

        # Assume total_pages is the same across all data types for simplicity (can be improved if needed)
        total_pages = self.data_types[0]['total_pages']  # This assumes that all endpoints have the same page count

        # Distribute pages for all workers
        page_ranges = self.distribute_pages_across_workers(total_pages, num_workers)

        # Create workers for each page range
        tasks = []
        for i, (start_page, end_page) in enumerate(page_ranges):
            logging.info(f"Worker {i} assigned page range: {start_page} to {end_page}")

            # Initialize the worker with the worker_id set in the constructor
            worker = DataIngestorWorker.options(num_cpus=cpu_per_worker).remote(self.base_url)

            # Assign tasks to process all endpoints for each worker
            logging.info(f"Worker {i} will process endpoints: {endpoints}")
            task = worker.ingest_and_save_data.remote(endpoints, file_base_path, start_page, end_page)
            tasks.append(task)

        # Wait for all tasks to complete
        ray.get(tasks)

        logging.info('Ingestion pipeline completed')

    def shutdown_pipeline(self):
        """
        Shutdown the Ray pipeline once all tasks are complete.
        """
        logging.info('Shutting down Ray pipeline')
        ray.shutdown()
