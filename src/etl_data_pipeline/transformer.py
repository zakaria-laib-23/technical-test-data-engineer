from pyspark.sql import functions as F
from etl_data_pipeline.base_stage import PipelineStage


class DataTransformer(PipelineStage):
    """Responsible for cleaning and transforming data."""

    def clean_tracks(self, tracks_df):
        """Cleans and filters tracks data."""
        return tracks_df.dropna(subset=["name", "artist", "duration", "genres"]) \
            .filter(F.col("duration") > "00:00:00")

    def clean_users(self, users_df):
        """Cleans and removes duplicates from users data."""
        return users_df.dropDuplicates(["email"])

    def clean_tracks(self, tracks_df):
        """Cleans and filters tracks data."""
        return tracks_df.dropna(subset=["name", "artist", "duration", "genres"]) \
            .filter(F.col("duration") > "00:00:00")

    def clean_users(self, users_df):
        """Cleans and removes duplicates from users data."""
        return users_df.dropDuplicates(["email"])

    def clean_listen_history(self, listen_history_df):
        """Cleans listen history data."""
        return listen_history_df.dropna()

    def join_data(self, users_df, listen_history_df, tracks_df):
        """Joins the users, listen history, and tracks data."""

        # Rename conflicting columns before join
        users_df = users_df.withColumnRenamed("id", "user_id") \
            .withColumnRenamed("updated_at", "user_updated_at") \
            .withColumnRenamed("created_at", "user_created_at")

        listen_history_df = listen_history_df.withColumnRenamed("updated_at", "history_updated_at") \
            .withColumnRenamed("created_at", "history_created_at")

        tracks_df = tracks_df.withColumnRenamed("id", "track_id") \
            .withColumnRenamed("updated_at", "track_updated_at") \
            .withColumnRenamed("created_at", "track_created_at")

        # Rename user_id in listen_history_df to avoid conflict
        listen_history_df = listen_history_df.withColumnRenamed("user_id", "history_user_id")

        # Perform the joins using left joins to include non-matching rows
        user_history_df = users_df.join(listen_history_df, users_df.user_id == listen_history_df.history_user_id,
                                        "left")
        full_data_df = user_history_df.join(tracks_df, tracks_df.track_id == listen_history_df.items, "left")

        return full_data_df

    def execute(self, data):
        """Executes the data transformation pipeline, including joining the data."""
        tracks_df = self.clean_tracks(data["tracks_df"])
        users_df = self.clean_users(data["users_df"])
        listen_history_df = self.clean_listen_history(data["listen_history_df"])

        # Join the data and return the full joined DataFrame
        full_data_df = self.join_data(users_df, listen_history_df, tracks_df)

        return full_data_df

    def execute(self, data):
        """Executes the data transformation pipeline."""
        tracks_df = self.clean_tracks(data["tracks_df"])
        users_df = self.clean_users(data["users_df"])
        full_data_df = self.join_data(users_df, data["listen_history_df"], tracks_df)
        return full_data_df
