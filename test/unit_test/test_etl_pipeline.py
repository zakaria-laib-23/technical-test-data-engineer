import pytest
from pyspark.sql import SparkSession
from etl_data_pipeline.loader import DataLoader
from etl_data_pipeline.transformer import DataTransformer
from etl_data_pipeline.writer import DataWriter

@pytest.fixture(scope="session")
def spark():
    """Fixture for creating a Spark session for tests."""
    spark = SparkSession.builder \
        .appName("ETL Pipeline Test") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_data_loader(mocker, spark):
    """Test the DataLoader component."""
    data_loader = DataLoader()

    # Mock the CSV loading function to return a dummy DataFrame
    mock_read = mocker.patch.object(spark.read, 'csv')
    mock_df = spark.createDataFrame([
        (1, 'Song A', 'Artist A', 'Album A', '2023-01-01', '2023-01-02'),
        (2, 'Song B', 'Artist B', 'Album B', '2023-01-01', '2023-01-02')
    ], ['id', 'name', 'artist', 'album', 'created_at', 'updated_at'])
    mock_read.return_value = mock_df

    data = data_loader.load_data()

    # Check if the DataFrames are not empty
    assert data['tracks_df'].count() > 0
    assert data['users_df'].count() > 0
    assert data['listen_history_df'].count() > 0

def test_data_transformer(spark):
    """Test the DataTransformer component."""
    transformer = DataTransformer()

    # Create mock DataFrames for transformation
    users_df = spark.createDataFrame([
        (1, 'John', 'Doe', 'john@example.com', 'Male', 'Rock'),
        (2, 'Jane', 'Doe', 'jane@example.com', 'Female', 'Pop')
    ], ['user_id', 'first_name', 'last_name', 'email', 'gender', 'favorite_genres'])

    tracks_df = spark.createDataFrame([
        (1, 'Song A', 'Artist A', 'Album A', '2023-01-01', '2023-01-02', '03:45', 'Rock'),
        (2, 'Song B', 'Artist B', 'Album B', '2023-01-01', '2023-01-02', '04:30', 'Pop')
    ], ['track_id', 'name', 'artist', 'album', 'created_at', 'updated_at', 'duration', 'genres'])

    listen_history_df = spark.createDataFrame([
        (1, 1, '2023-01-01', '2023-01-02'),
        (2, 2, '2023-01-01', '2023-01-02')
    ], ['user_id', 'items', 'created_at', 'updated_at'])

    data = {
        "users_df": users_df,
        "tracks_df": tracks_df,
        "listen_history_df": listen_history_df
    }

    # Test the transformation logic
    transformed_data = transformer.execute(data)

    # Check if the result DataFrame is not empty
    assert transformed_data.count() > 0


def test_data_writer(mocker, spark):
    """Test the DataWriter component."""
    writer = DataWriter()

    # Create a mock DataFrame
    transformed_df = spark.createDataFrame([
        (1, 'John Doe', 'Song A', 'Artist A', 'Rock'),
        (2, 'Jane Doe', 'Song B', 'Artist B', 'Pop')
    ], ['user_id', 'name', 'artist', 'album', 'favorite_genres'])

    # Mock the save_to_csv function
    mock_save_to_csv = mocker.patch('etl_data_pipeline.writer.DataWriter.save_to_csv')

    # Simulate saving the DataFrame
    writer.execute(transformed_df)

    # Ensure the save_to_csv method was called once
    mock_save_to_csv.assert_called_once()
