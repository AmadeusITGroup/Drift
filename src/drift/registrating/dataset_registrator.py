import logging
from datetime import datetime

from pydataio.job_config import JobConfig
from pydataio.transformer import Transformer
from pyspark.sql import SparkSession

from drift.registrating.data_asset_registrator import DataAssetRegistrator
from drift.tools.azml import init_ml_flow_utils

logger = logging.getLogger(__name__)

class DatasetRegistrator(Transformer):

    def __init__(self):
        return

    def featurize(self, jobConfig: JobConfig, spark: SparkSession, additionalArgs: dict = None):
        """
        Register a new version of the data for training
        Args:
            jobConfig: the job configuration
            spark: the spark session
        """
        parameters = self.load_parameters(jobConfig)
        version, delta_timestamp = self.compute_version()

        ml_flow_utils = init_ml_flow_utils(jobConfig, additionalArgs["vault_name"])

        data_asset_registrator = DataAssetRegistrator(ml_flow_utils.ml_client, ml_flow_utils.sp_config, parameters, version, delta_timestamp)
        data_asset_registrator.register_dataset()

        self.publish_new_version(version)


    def publish_new_version(self, new_version: str):
        """
        Publish the new version of the data asset to databricks
        Args:
            new_version: the new version

        """
        from databricks.sdk.runtime import dbutils

        dbutils.jobs.taskValues.set(key= "data_asset_version", value= new_version)


    @staticmethod
    def load_parameters(jobConfig: JobConfig) -> dict[str, str]:
        """
        Load the parameters from the job configuration
        Args:
            jobConfig: the job configuration
        """
        parameters = {
            'subscription_id': jobConfig.parameters["azml"]["subscriptionId"],
            'resource_group': jobConfig.parameters["azml"]["resourceGroup"],
            'ml_workspace_name':  jobConfig.parameters["azml"]["mlWorkspaceName"],
            'storage_account_name': jobConfig.parameters["storageAccountName"],
            'container_name': jobConfig.parameters["containerName"],
            'container_path': jobConfig.parameters["containerDataPath"]
        }

        return parameters



    @staticmethod
    def compute_version()-> (str, str):
        """
        Compute the new version of the data assets
        """
        current_datetime = datetime.now()
        delta_timestamp = current_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        version = current_datetime.strftime("%Y%m%d%H%M%S" )

        return version, delta_timestamp