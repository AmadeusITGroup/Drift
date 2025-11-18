import logging
import random
import re
import string
from datetime import datetime

from azure.ai.ml import MLClient
from azure.ai.ml.entities import PipelineJob
from pydataio.job_config import JobConfig
from pydataio.transformer import Transformer
from pyspark.sql import SparkSession

from drift.tools.azml import init_ml_flow_utils
from drift.retraining.job_group import JobGroup
from drift.retraining.training_status_refresher import TrainingStatusRefresher

logger = logging.getLogger(__name__)


class ModelRetrainer(Transformer):
    jobConfig: JobConfig
    job_name_pattern: str
    training_status_refresher: TrainingStatusRefresher

    def __init__(self):
        return

    def featurize(self, jobConfig: JobConfig, spark: SparkSession, additionalArgs: dict = None):
        self.jobConfig = jobConfig

        self.compute_jobname_pattern(additionalArgs)

        logger.info(self.jobConfig.parameters["dataAssets"])

        ml_flow_utils = init_ml_flow_utils(jobConfig, additionalArgs["vault_name"])
        self.training_status_refresher = TrainingStatusRefresher(jobConfig, ml_flow_utils.ml_client)

        jobs_to_retrain = self.retrieve_jobs_to_retrain(ml_flow_utils.ml_client)
        created_jobs = self.retrain_models(ml_flow_utils.ml_client, jobs_to_retrain, additionalArgs)
        self.check_success(created_jobs)

    def compute_jobname_pattern(self, additionalArgs: dict):
        """
        Compute the job name pattern
        Args:
            additionalArgs: the additional arguments with the optional group name
        """

        group_name_pattern = "[a-z,0-9]{2,}"
        if additionalArgs.get("model_name_prefix", None) is not None:
            model_name_prefix = additionalArgs["model_name_prefix"].replace(",", "|")
            logger.info("Retrain models for %s  only.", model_name_prefix)
            group_name_pattern = f"({model_name_prefix})"

        self.job_name_pattern = r"^" + group_name_pattern + "_[0-9]{14}_.*$"

    def retrain_models(self, ml_client: MLClient, jobs_to_retrain: list[JobGroup], additionalArgs: dict) -> list[PipelineJob]:
        """
        Retrain the models
        Args:
            ml_client: the ml client
            jobs_to_retrain: the jobs to retrain
            additionalArgs: the additional arguments

        Returns: the newly created jobs for retraining
        """

        data_asset_version = additionalArgs["data_asset_version"]
        logger.info("Retrain models with data asset version %s", data_asset_version)

        created_jobs: list[PipelineJob] = []

        for group_job in jobs_to_retrain:
            logger.info("Retrain model for group %s", group_job.group_name)

            based_job = group_job.job
            self.update_data_assets(based_job, data_asset_version)
            based_job.name = None
            based_job.display_name = self.create_new_display_name(group_job.group_name)
            created_job = ml_client.jobs.create_or_update(based_job)

            logger.info("Created job %s", created_job.display_name)
            logger.debug(created_job)

            created_jobs.append(created_job)

        return created_jobs

    def check_success(self, jobs: list[PipelineJob]):
        """
        Check if the jobs are successful
        Args:
            jobs: the jobs to checks
        """
        failed_jobs = self.training_status_refresher.wait_training(jobs)
        if len(failed_jobs) > 0:
            for failed_job in failed_jobs:
                logger.error("Job %s failed.", failed_job.display_name)

            raise Exception("Some jobs failed.")

    @staticmethod
    def create_new_display_name(group_name: str):
        """
        Create a new display name for the job
        Args:
            group_name: the group name

        Returns: the new display name
        """
        random_string = "".join(random.choices(string.ascii_letters + string.digits, k=6))

        current_datetime = datetime.now()
        return f"{group_name}_{current_datetime.strftime('%Y%m%d%H%M%S')}_{random_string}"

    def update_data_assets(self, job: PipelineJob, data_asset_version: str):
        """
        Update the data assets
        Args:
            job: the job
            data_asset_version: the data asset version
        """

        for data_asset in self.jobConfig.parameters["dataAssets"]:
            logger.info("Update data asset %s for based job %s", data_asset["name"], job.display_name)
            job.inputs[data_asset["name"]].path = f"{data_asset['value']}:{data_asset_version}"

    def retrieve_jobs_to_retrain(self, ml_client: MLClient) -> list[JobGroup]:
        """
        Retrieve the jobs to retrain
        Args:
            ml_client: the ml client

        Returns: the jobs to retrain
        """

        job_to_schedule: list[PipelineJob] = list(ml_client.jobs.list())
        logger.debug("Retrieved %s jobs.", len(job_to_schedule))

        job_to_schedule = list(filter(self.is_in_scope, job_to_schedule))
        logger.debug("Retrieved %s jobs in the scope:", len(job_to_schedule))

        jobs_to_retrain_dict: dict[str, JobGroup] = {}
        for job in job_to_schedule:
            names = re.split(r"_", job.display_name)
            group_name = names[0]
            training_timestamp = names[1]

            if jobs_to_retrain_dict.get(group_name) is None:
                logger.info("Add new job %s trained at %s to group %s ", job.display_name, training_timestamp, group_name)
                jobs_to_retrain_dict[group_name] = JobGroup(group_name, training_timestamp, job)
            else:
                if jobs_to_retrain_dict[group_name].is_older_than(training_timestamp):
                    logger.info("Update group %s with newer job %s trained at %s", group_name, job.display_name, training_timestamp)
                    jobs_to_retrain_dict[group_name] = JobGroup(group_name, training_timestamp, job)

        jobs_to_retrain = list(jobs_to_retrain_dict.values())
        logger.debug(jobs_to_retrain)

        if len(job_to_schedule) == 0:
            logger.info("No jobs to retrain.")
            raise Exception("No jobs in scope to retrain.")

        return jobs_to_retrain

    @staticmethod
    def is_data_asset_in_scope(job: PipelineJob, data_asset_name: str, data_asset: str) -> bool:
        """
        Check if the job referencing the monitored data asset
        Args:
            job: the job
            data_asset_name: the name of the data asset
            data_asset:  the data asset

        Returns: True if the data asset is in the scope of the job
        """

        if job.inputs.get(data_asset_name, None) is None:
            return False

        return job.inputs[data_asset_name].path.startswith(data_asset)

    def are_data_assets_in_scope(self, job: PipelineJob) -> bool:
        """
        Check if the job is in the scope of the data assets
        Args:
            job: the job

        Returns: True if the job is in the scope of the data assets
        """

        for data_asset in self.jobConfig.parameters["dataAssets"]:
            if not self.is_data_asset_in_scope(job, data_asset["name"], data_asset["value"]):
                return False

        return True

    def is_in_scope(self, job: PipelineJob) -> bool:
        """
        Check if the job is in the scope of the model retraining
        Args:
            job: the job

        Returns: True if the job is in the scope of the model retraining
        """

        if re.match(self.job_name_pattern, job.display_name):
            return self.are_data_assets_in_scope(job)
        else:
            return False
