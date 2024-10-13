import pytest
from unittest.mock import patch, AsyncMock
import httpx
from ingestor.api_client import APIClient  # Replace with the correct import path


@pytest.mark.asyncio
@patch("httpx.AsyncClient.get")
async def test_fetch_paginated_data(mock_get):
    # Create a mock request object
    request = httpx.Request("GET", "https://api.example.com/data")

    # First mock response (page 1 with data)
    first_response = httpx.Response(
        status_code=200,
        json={"items": [{"id": 1, "name": "Test Item"}]},
        request=request
    )

    # Second mock response (page 2 with no data to stop pagination)
    second_response = httpx.Response(
        status_code=200,
        json={"items": []},
        request=request
    )

    # Set up the mock to return the first response, then the second response
    mock_get.side_effect = [first_response, second_response]

    # Instantiate the API client
    client = APIClient(base_url="https://api.example.com")

    # Call the method
    data = await client.fetch_paginated_data(endpoint="/data")

    # Assertions
    assert isinstance(data, list)
    assert len(data) == 1  # Only one item should be returned
    assert data[0]["id"] == 1
