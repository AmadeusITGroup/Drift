import logging
import os

from azure.ai.ml import MLClient
from azure.ai.ml.entities import ServicePrincipalConfiguration
from azure.identity import ClientSecretCredential
from pydataio.job_config import JobConfig

logger = logging.getLogger(__name__)

class AzMLConfig:
    """
    Class to represent the Azure ML configuration
    """

    def __init__(self, job_config: JobConfig, vault_name: str):
        azml_config = job_config.parameters["azml"]
        self.subscription_id = azml_config["subscriptionId"]
        self.resource_group_name = azml_config["resourceGroup"]
        self.workspace_name = azml_config["mlWorkspaceName"]
        self.vault_name = vault_name

class MlFlowUtils:
    """
    Class to interact with Azure ML
    """
    sp_config: ServicePrincipalConfiguration
    ml_client: MLClient
    mlflow_tracking_uri: str

    def __init__(self, az_ml_config: AzMLConfig):
        """
        Initialize the class
        Args:
            az_ml_config: Azure ML configuration
        """
        from databricks.sdk.runtime import dbutils

        tenant_id = dbutils.secrets.get(scope=az_ml_config.vault_name, key='TenantID')
        client_id = dbutils.secrets.get(scope=az_ml_config.vault_name, key="ApplicationID")
        client_secret = dbutils.secrets.get(scope=az_ml_config.vault_name, key="ApplicationPassword")

        client_secret_credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )

        self.ml_client = MLClient(
            credential=client_secret_credential,
            subscription_id=az_ml_config.subscription_id,
            resource_group_name=az_ml_config.resource_group_name,
            workspace_name=az_ml_config.workspace_name,
        )

        self.sp_config = ServicePrincipalConfiguration(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        os.environ["AZURE_TENANT_ID"] = tenant_id
        os.environ["AZURE_CLIENT_ID"] = client_id
        os.environ["AZURE_CLIENT_SECRET"] = client_secret

        self.mlflow_tracking_uri = self.ml_client.workspaces.get(
            self.ml_client.workspace_name
        ).mlflow_tracking_uri

def init_ml_flow_utils(jobConfig: JobConfig, vault_name: str) -> MlFlowUtils:
    """
    Initialize the MLFlow utils
    Args:
        jobConfig: the job configuration
        vault_name: key vault nane

    Returns: the MLFlow utils
    """
    az_ml_config = AzMLConfig(jobConfig, vault_name)
    return MlFlowUtils(az_ml_config)