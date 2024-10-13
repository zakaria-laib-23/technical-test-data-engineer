import pytest
from unittest.mock import patch, AsyncMock

from ingestor.api_client import APIClient


@pytest.mark.asyncio
@patch("httpx.AsyncClient.get")
async def test_fetch_paginated_data(mock_get):
    # Mock the response from the API
    mock_response = AsyncMock()
    mock_response.json.return_value = {"items": [{"id": 1, "name": "Test Item"}]}
    mock_response.raise_for_status = AsyncMock()  # Simulates no error
    mock_get.return_value = mock_response

    client = APIClient(base_url="https://api.example.com")
    data = await client.fetch_paginated_data(endpoint="/data")

    assert isinstance(data, list)
    assert len(data) > 0
    assert data[0]["id"] == 1
