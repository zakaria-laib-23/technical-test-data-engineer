from typing import AsyncGenerator

from data_ingestion_pipeline.api_client import APIClient


class DataIngestor:
    """
    Class responsible for ingesting data from specific endpoints using the APIClient.
    """
    def __init__(self, client: APIClient):
        self.client = client

    async def fetch_paginated_data_stream(self, endpoint: str, start_page: int = 1, end_page: int = None) -> AsyncGenerator:
        """
        Stream paginated data from a given endpoint, within the specified page range.

        Args:
            endpoint (str): The API endpoint to fetch data from.
            start_page (int): The starting page number.
            end_page (int): The ending page number (inclusive). If None, it will continue until no more data.

        Yields:
            List[dict]: A list of items from each page.
        """
        page = start_page

        while True:
            if end_page is not None and page > end_page:
                break

            try:
                # Fetch a single page of data using APIClient
                response_data = await self.client.fetch_page(endpoint, page)

                if not response_data.get('items'):
                    break  # Stop if there are no more items

                yield response_data['items']
                page += 1

            except Exception as exc:
                print(f"Error fetching page {page}: {exc}")
                raise exc
