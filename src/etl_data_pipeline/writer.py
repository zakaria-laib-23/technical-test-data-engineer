import logging

from etl_data_pipeline.base_stage import PipelineStage


class DataWriter(PipelineStage):
    """Responsible for saving the transformed data."""

    def save_to_csv(self, df, file_name):
        """Saves the DataFrame to a CSV file."""
        logging.info(f"Saving data to {file_name}...")
        df.coalesce(1).write.csv(f"output/{file_name}", mode="overwrite", header=True)
        logging.info(f"Data saved to {file_name}.")

    def execute(self, data):
        """Saves the transformed data after transformation."""
        self.save_to_csv(data, "transformed_data")