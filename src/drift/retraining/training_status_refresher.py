import time
import logging
from datetime import datetime, timedelta

from azure.ai.ml import MLClient
from azure.ai.ml.entities import PipelineJob
from pydataio.job_config import JobConfig

logger = logging.getLogger(__name__)

class TrainingStatusRefresher:
    ml_client: MLClient
    timeout_delay: int
    refresh_delay: int

    def __init__(self, job_config: JobConfig, ml_client: MLClient):
        self.timeout_delay = int(job_config.parameters["refreshTimeout"])
        self.refresh_delay = int(job_config.parameters["refreshDelay"])
        self.ml_client = ml_client



    def wait_training(self, new_jobs: list[PipelineJob]) -> list[PipelineJob]:
        """
        Wait for the training jobs to completed
        Args:
            new_jobs: the new jobs to wait for

        Returns: the not completed jobs
        """

        timeout = datetime.now() + timedelta(seconds=self.timeout_delay)
        logger.info(f"Waiting for {self.timeout_delay} seconds, until {timeout}")

        updated_jobs = new_jobs

        has_to_wait = True
        while has_to_wait:
            updated_jobs = self.refresh_job_status(updated_jobs)
            for job in updated_jobs:
                logger.info(f"Wait for job {job.display_name} to complete.")

            has_to_wait = len([updt_jb for updt_jb in updated_jobs if updt_jb.status != "Failed"]) > 0
            logger.debug(f"Has to wait {has_to_wait}")

            self.check_timeout_reached(timeout)

            time.sleep(self.refresh_delay)

        return updated_jobs

    def check_timeout_reached(self, timeout: datetime) :
        """
        Check if the timeout is reached
        Args:
            timeout: the timeout

        Returns: True if the timeout is reached
        """
        if datetime.now() > timeout:
            raise Exception("Timeout reached")

    def refresh_job_status(self,  jobs: list[PipelineJob]) -> list[PipelineJob]:
        """
        Refresh the job status
        Args:
            jobs: the jobs

        Returns: the refreshed jobs not completed
        """

        refreshed_jobs = []
        for job in jobs:
            refreshed_job = self.ml_client.jobs.get(job.name)
            logger.info(f"Training job {refreshed_job.display_name} ({refreshed_job.name}): [{refreshed_job.status}]")

            if refreshed_job.status != "Completed":
                logger.debug(f"Add {refreshed_job.display_name} ({refreshed_job.name}) to waiting list")
                refreshed_jobs.append(refreshed_job)

        return refreshed_jobs