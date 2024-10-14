import pytest
from unittest.mock import AsyncMock, patch
import httpx
from ingestor.api_client import APIClient
from ingestor.data_ingestor import DataIngestor  # Adjust the import paths based on your project structure


# Fixture for mocking APIClient.fetch_page
@pytest.fixture
def mock_api_client():
    # Mock APIClient
    client = APIClient(base_url="https://api.example.com")
    client.fetch_page = AsyncMock()
    return client


@pytest.mark.asyncio
async def test_fetch_paginated_data_stream_within_page_range(mock_api_client):
    """
    Test that DataIngestor fetches data for a specified range of pages.
    """
    # Mock the response for pages 1, 2, and 3
    mock_page_1 = {"items": [{"id": 1, "name": "Item 1"}]}
    mock_page_2 = {"items": [{"id": 2, "name": "Item 2"}]}
    mock_page_3 = {"items": [{"id": 3, "name": "Item 3"}]}

    # Set the side effect for fetch_page to simulate responses for pages 1, 2, and 3
    mock_api_client.fetch_page.side_effect = [mock_page_1, mock_page_2, mock_page_3]

    # Instantiate the DataIngestor
    ingestor = DataIngestor(client=mock_api_client)

    # Fetch data between pages 1 and 3
    collected_items = []
    async for page_data in ingestor.fetch_paginated_data_stream("/test", start_page=1, end_page=3):
        collected_items.extend(page_data)

    # Assertions
    assert len(collected_items) == 3  # Should have collected items from 3 pages
    assert collected_items[0]["id"] == 1
    assert collected_items[1]["id"] == 2
    assert collected_items[2]["id"] == 3

    # Ensure fetch_page was called three times
    assert mock_api_client.fetch_page.call_count == 3
    mock_api_client.fetch_page.assert_any_call("/test", 1)
    mock_api_client.fetch_page.assert_any_call("/test", 2)
    mock_api_client.fetch_page.assert_any_call("/test", 3)


@pytest.mark.asyncio
async def test_fetch_paginated_data_stream_until_no_more_data(mock_api_client):
    """
    Test that DataIngestor fetches data until there are no more items returned by the API.
    """
    # Mock the response for pages 1 and 2, with page 3 returning no items
    mock_page_1 = {"items": [{"id": 1, "name": "Item 1"}]}
    mock_page_2 = {"items": [{"id": 2, "name": "Item 2"}]}
    mock_page_3 = {"items": []}  # No more data after page 2

    # Set the side effect for fetch_page to simulate responses for pages 1, 2, and no data on page 3
    mock_api_client.fetch_page.side_effect = [mock_page_1, mock_page_2, mock_page_3]

    # Instantiate the DataIngestor
    ingestor = DataIngestor(client=mock_api_client)

    # Fetch data starting from page 1 until no more items
    collected_items = []
    async for page_data in ingestor.fetch_paginated_data_stream("/test", start_page=1):
        collected_items.extend(page_data)

    # Assertions
    assert len(collected_items) == 2  # Should have collected items from 2 pages
    assert collected_items[0]["id"] == 1
    assert collected_items[1]["id"] == 2

    # Ensure fetch_page was called three times (page 3 returned no items)
    assert mock_api_client.fetch_page.call_count == 3
    mock_api_client.fetch_page.assert_any_call("/test", 1)
    mock_api_client.fetch_page.assert_any_call("/test", 2)
    mock_api_client.fetch_page.assert_any_call("/test", 3)


@pytest.mark.asyncio
async def test_fetch_paginated_data_stream_error_handling(mock_api_client):
    """
    Test that DataIngestor handles errors gracefully during data fetching.
    """
    # Mock the response for page 1, followed by an HTTP error on page 2
    mock_page_1 = {"items": [{"id": 1, "name": "Item 1"}]}

    # Set the side effect for fetch_page to simulate a normal response for page 1 and an error on page 2
    mock_api_client.fetch_page.side_effect = [mock_page_1, httpx.HTTPStatusError("Error", request=None, response=None)]

    # Instantiate the DataIngestor
    ingestor = DataIngestor(client=mock_api_client)

    # Attempt to fetch data and expect an exception for page 2
    collected_items = []
    with pytest.raises(httpx.HTTPStatusError):
        async for page_data in ingestor.fetch_paginated_data_stream("/test", start_page=1):
            collected_items.extend(page_data)

    # Ensure fetch_page was called twice (once for page 1 and once for the error on page 2)
    assert mock_api_client.fetch_page.call_count == 2
    mock_api_client.fetch_page.assert_any_call("/test", 1)
    mock_api_client.fetch_page.assert_any_call("/test", 2)

    # Ensure that only data from page 1 was collected
    assert len(collected_items) == 1
    assert collected_items[0]["id"] == 1
