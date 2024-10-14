import ray
import pandas as pd
import os
from typing import List

from ingestor.api_client import APIClient
from ingestor.data_ingestor import DataIngestor

class DataIngestorActor:
    """
    A Ray worker class that dynamically ingests a range of pages using the DataIngestor.
    Each worker is responsible for fetching and saving a specific range of pages.
    """

    def __init__(self, base_url: str):
        """
        Initialize the worker with a unique worker ID and the base URL for the API.
        """
        self.client = APIClient(base_url)
        self.ingestor = DataIngestor(self.client)

    async def ingest_and_save_data(self, endpoints: List[str], file_base_path: str, start_page: int, end_page: int):
        """
        Ingest data from multiple endpoints by fetching a specific range of pages using the DataIngestor
        and save to a unique CSV file. Each worker will create a file with the endpoint and worker index in the file name.

        Args:
            endpoints (List[str]): A list of API endpoints to process.
            file_base_path (str): The base path where the CSV files will be saved.
            start_page (int): The starting page to ingest.
            end_page (int): The ending page to ingest.
        """
        # Loop through each endpoint
        for endpoint in endpoints:
            # Create a unique file path for each endpoint and worker
            file_path = f"{file_base_path}_{endpoint.replace('/', '')}.csv"
            # Fetch and write data for the given endpoint
            async for page_data in self.ingestor.fetch_paginated_data_stream(endpoint, start_page, end_page):
                self.save_to_csv(page_data, file_path)

    def save_to_csv(self, data: List, file_path: str):
        """
        Save data to a CSV file. Appends to the file if it already exists.
        """
        df = pd.DataFrame(data)
        df.to_csv(file_path, mode='a', header=not os.path.isfile(file_path), index=False)

@ray.remote
class DataIngestorWorker(DataIngestorActor):
    pass