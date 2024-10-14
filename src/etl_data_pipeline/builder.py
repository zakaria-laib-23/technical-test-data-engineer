class PipelineBuilder:
    """Builds and executes the pipeline stages."""

    def __init__(self):
        self.stages = []

    def add_stage(self, stage):
        """Adds a stage to the pipeline."""
        self.stages.append(stage)
        return self

    def build_default_pipeline(self):
        """Builds the default ETL pipeline with predefined stages."""
        from etl_data_pipeline.writer import DataWriter
        from etl_data_pipeline.loader import DataLoader
        from etl_data_pipeline.transformer import DataTransformer

        # Instantiate the stages
        writer = DataWriter()
        loader = DataLoader()
        transformer = DataTransformer()

        # Add default stages to the pipeline
        self.add_stage(loader) \
            .add_stage(transformer) \
            .add_stage(writer)

        return self

    def execute(self):
        """Executes all the stages in the pipeline."""
        data = None
        for stage in self.stages:
            data = stage.execute(data)
