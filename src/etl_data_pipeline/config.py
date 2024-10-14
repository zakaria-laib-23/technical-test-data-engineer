
class PipelineConfig:
    TRACKS_FILE_PATH = "data_ingestion_output_tracks.csv"
    USERS_FILE_PATH = "data_ingestion_output_users.csv"
    LISTEN_HISTORY_FILE_PATH = "data_ingestion_output_listen_history.csv"
    OUTPUT_PATH = "output_data"
    APP_NAME = "MusicDataPipeline"
    MASTER = "local[*]"
    PARTITION_COLUMN = "user_id"  # Example for partitioning
    PAGE_SIZE = 100
    TOTAL_RECORDS = 10000
