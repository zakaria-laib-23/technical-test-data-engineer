import httpx
import asyncio

class APIClient:
    def __init__(self, base_url: str, retries: int = 3, timeout: int = 30):
        self.base_url = base_url
        self.retries = retries
        self.timeout = timeout

    async def fetch_page(self, endpoint: str, page: int):
        """
        Fetch a single page of data from the API with retries on failure.
        """
        retry_count = 0
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            while True:
                try:
                    response = await client.get(f"{self.base_url}{endpoint}", params={"page": page})
                    response.raise_for_status()
                    return response.json()
                except (httpx.RequestError, httpx.HTTPStatusError) as exc:
                    if retry_count < self.retries:
                        retry_count += 1
                        print(f"Error: {exc}. Retrying {retry_count}/{self.retries}...")
                        await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                    else:
                        print(f"Max retries exceeded for {endpoint}, page {page}.")
                        raise exc
