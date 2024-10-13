import httpx
import asyncio
from typing import List

class APIClient:
    def __init__(self, base_url: str, retries: int = 3, timeout: int = 10):
        self.base_url = base_url
        self.retries = retries
        self.timeout = timeout

    async def fetch_paginated_data(self, endpoint: str, page_size: int = 100) -> List[dict]:
        """
        Fetch paginated data with retries on failure.
        """
        all_data = []
        page = 1
        retry_count = 0

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while True:
                try:
                    response = await client.get(f"{self.base_url}{endpoint}", params={"page": page, "size": page_size})
                    response.raise_for_status()
                    response_data = response.json()

                    if not response_data['items']:
                        break

                    all_data.extend(response_data['items'])
                    page += 1
                    retry_count = 0
                except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                    if retry_count < self.retries:
                        retry_count += 1
                        print(f"Error: {exc}. Retrying {retry_count}/{self.retries}...")
                        # Exponential backoff
                        await asyncio.sleep(2 ** retry_count)
                    else:
                        print(f"Max retries exceeded for {endpoint}.")
                        raise exc

        return all_data