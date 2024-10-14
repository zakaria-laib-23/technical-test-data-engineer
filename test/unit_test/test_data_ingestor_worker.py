import pytest
from unittest.mock import patch, AsyncMock, call
from ingestor.data_ingestor_worker import DataIngestorActor


@pytest.mark.asyncio
@patch("pandas.DataFrame.to_csv")  # Mock the file writing
@patch("ingestor.data_ingestor.DataIngestor.fetch_paginated_data_stream")  # Mock the data fetcher
@patch("os.path.isfile")  # Mock the file existence check
async def test_ingest_and_save_data(mock_isfile, mock_fetch_paginated_data, mock_to_csv):
    # Mock the file existence - assume file does not exist for the first page, and exists after
    mock_isfile.side_effect = [False, True]

    # Mock the paginated data response
    mock_fetch_paginated_data.return_value = AsyncMock()
    mock_fetch_paginated_data.return_value.__aiter__.return_value = [
        [{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}],  # Page 1
        [{"id": 3, "name": "Item 3"}, {"id": 4, "name": "Item 4"}],  # Page 2
    ]

    # Create an instance of the actor (directly without Ray)
    actor = DataIngestorActor("https://api.example.com")

    # Call the method to ingest data and save it
    await actor.ingest_and_save_data(
        endpoints=["/test_endpoint"],
        file_base_path="test_data",
        start_page=1,
        end_page=2
    )

    # Ensure that the correct file path was used
    expected_file_path = "test_data_test_endpoint.csv"

    # Ensure that pandas to_csv method was called twice (once for each page)
    assert mock_to_csv.call_count == 2

    # Check that the DataFrame's to_csv method was called with the correct arguments
    mock_to_csv.assert_has_calls([
        call(expected_file_path, mode='a', header=True, index=False),  # First page, header=True
        call(expected_file_path, mode='a', header=False, index=False)  # Second page, header=False
    ])

    # Ensure that the paginated data was fetched from the correct endpoint
    mock_fetch_paginated_data.assert_called_once_with("/test_endpoint", 1, 2)
