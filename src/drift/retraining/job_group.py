from azure.ai.ml.entities import PipelineJob


class JobGroup:
    """
    A group of jobs with the same name
    """

    group_name: str
    training_timestamp: int
    job: PipelineJob

    def __init__(self, group_name: str, training_timestamp: int, job: PipelineJob):
        self.group_name = group_name
        self.training_timestamp = training_timestamp
        self.job = job

    def is_older_than(self, other_training_timestamp: int) -> bool:
        return self.training_timestamp < other_training_timestamp

    def __str__(self):
        return f"JobGroup(training_timestamp={self.training_timestamp}, job={self.job})"

    def __rep__(self):
        return f"JobGroup(training_timestamp={self.training_timestamp}, job={self.job})"
