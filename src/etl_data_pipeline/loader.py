import logging
import multiprocessing

from pyspark.sql import SparkSession
from etl_data_pipeline.config import PipelineConfig
from etl_data_pipeline.base_stage import PipelineStage

class DataLoader(PipelineStage):
    """Responsible for loading CSV data into Spark DataFrames."""
    def __init__(self):
        self.spark = self.create_spark_session()

    @staticmethod
    def create_spark_session():
        """Creates and configures the Spark session with dynamic CPU scaling."""
        # Detect the number of available CPUs
        num_cpus = multiprocessing.cpu_count()
        logging.info(f"Number of CPUs detected: {num_cpus}")

        # Create Spark session using the number of CPUs detected
        spark = (SparkSession.builder \
            .appName("Data Pipeline with Dynamic CPU Scaling") \
            .master(f"local[{num_cpus}]")\
            .getOrCreate())

        return spark


    def load_data(self):
            """Loads the CSV data into Spark DataFrames."""
            logging.info("Loading data...")

            tracks_df = self.spark.read.csv(PipelineConfig.TRACKS_FILE_PATH, header=True, inferSchema=True)
            users_df = self.spark.read.csv(PipelineConfig.USERS_FILE_PATH, header=True, inferSchema=True)
            listen_history_df = self.spark.read.csv(PipelineConfig.LISTEN_HISTORY_FILE_PATH, header=True, inferSchema=True)

            # Log the number of records in each DataFrame
            logging.info(f"Loaded {tracks_df.count()} tracks.")
            logging.info(f"Loaded {users_df.count()} users.")
            logging.info(f"Loaded {listen_history_df.count()} listen history records.")

            return {"tracks_df": tracks_df, "users_df": users_df, "listen_history_df": listen_history_df}

    def execute(self, data=None):
        """Executes the loading of data."""
        return self.load_data()

