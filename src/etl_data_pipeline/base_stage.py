from abc import ABC, abstractmethod


class PipelineStage(ABC):
    """
    Abstract base class for pipeline stages.
    Each stage (loading, transforming, analyzing, saving) must implement the `execute` method.
    """

    @abstractmethod
    def execute(self, data=None):
        pass
