import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from data_ingestion_pipeline.api_client import APIClient  # Update this import to match your actual module path

@pytest.mark.asyncio
async def test_fetch_page():
    """
    Test the fetch_page method to ensure it retrieves a single page correctly.
    """
    # Create an instance of APIClient
    client = APIClient(base_url="https://api.example.com")

    # Mock the response for the first page
    mock_response_data = {
        "items": [{"id": 1, "name": "Test Item"}]
    }

    # Patch the httpx.AsyncClient.get method to return the mocked response
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        # Create a mock response object with the json method returning the mock_response_data
        mock_response = MagicMock()
        mock_response.json.return_value = mock_response_data

        mock_get.return_value = mock_response

        # Call the fetch_page method
        response = await client.fetch_page(endpoint="/test", page=1)

        # Assertions
        assert response == mock_response_data
        assert mock_get.called_once()
        mock_get.assert_called_with("https://api.example.com/test", params={"page": 1})
