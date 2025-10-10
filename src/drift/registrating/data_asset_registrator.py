import logging

import mltable
from azure.ai.ml import MLClient
from azure.ai.ml.constants import AssetTypes
from azure.ai.ml.entities import Data, AzureDataLakeGen2Datastore, ServicePrincipalConfiguration

logger = logging.getLogger(__name__)

class DataAssetRegistrator:
    """
    Register a new version of the data for training
    """
    ml_client: MLClient
    sp_config: ServicePrincipalConfiguration
    parameters: dict[str, str]
    mltable_name: str
    data_asset_uri: str
    version: str
    delta_timestamp: str

    def __init__(self, ml_client: MLClient, sp_config: ServicePrincipalConfiguration, parameters: dict[str, str], version: str, delta_timestamp: str):
        """
        Constructor
        Args:
            ml_client: the ML client
            sp_config: the service principal configuration
            parameters: the job parameters
        """
        self.ml_client = ml_client
        self.sp_config = sp_config
        self.version = version
        self.delta_timestamp = delta_timestamp
        self.parameters = parameters

        path_asset_name = parameters['container_path'].replace("/", "-").lstrip("-").rstrip("-")
        self.mltable_name = f"{parameters['container_name']}-{path_asset_name}-mltable"
        self.data_asset_uri = f"{parameters['container_name']}-{path_asset_name}-uri"

    def register_dataset(self):
        """
        Register the dataset in the ML workspace

        """
        datastore_name = self.parameters['container_name'].replace("-", "_")

        store = AzureDataLakeGen2Datastore(
            name=datastore_name,
            description="Datastore pointing to an Azure Data Lake Storage Gen2.",
            account_name=self.parameters['storage_account_name'],
            filesystem=self.parameters['container_name'],
            credentials=self.sp_config,

        )

        created_datastore = self.ml_client.create_or_update(store)
        logger.debug(f"Datastore created or updated: {created_datastore}")

        azml_path_datastore = f"azureml://subscriptions/{self.parameters['subscription_id']}/resourcegroups/{self.parameters['resource_group']}/workspaces/{self.parameters['ml_workspace_name']}/datastores/{datastore_name}/paths/{self.parameters['container_path']}"

        self.register_mltable(azml_path_datastore)
        self.register_uri_data_asset(azml_path_datastore)



    def register_mltable(self, azml_path_datastore: str):
        """
        Register the mltable in the ML workspace
        Args:
            azml_path_datastore: the path to the ML data store
        """

        table = mltable.from_delta_lake(azml_path_datastore, timestamp_as_of=self.delta_timestamp)
        table.save(f"./{self.mltable_name}")
        logger.debug(f"MLTable saved: {self.mltable_name}")

        mltable_data_asset = Data(
            path=f"./{self.mltable_name}",
            type=AssetTypes.MLTABLE,
            description="data asset using mltable.",
            name=self.mltable_name,
            version=self.version
        )
        self.ml_client.data.create_or_update(mltable_data_asset)
        logger.debug(f"MLTable data asset created or updated: {mltable_data_asset}")

    def register_uri_data_asset(self, azml_path_datastore: str):
        """
        Register the URI data asset in the ML workspace
        Args:
            azml_path_datastore: the path to the ML data store

        """
        uri_data_asset = Data(
            path=azml_path_datastore,
            type=AssetTypes.URI_FOLDER,
            description="Uri Data Asset",
            name=self.data_asset_uri,
            version=self.version
        )
        self.ml_client.data.create_or_update(uri_data_asset)
        logger.debug(f"URI Data asset created or updated: {uri_data_asset}")