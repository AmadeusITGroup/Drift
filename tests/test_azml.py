import logging
import os
from unittest.mock import MagicMock, Mock, patch

import pytest

from drift.tools.azml import AzMLConfig, MlFlowUtils, init_ml_flow_utils


@pytest.fixture
def mock_job_config():
    """Create a mock JobConfig object"""
    job_config = Mock()
    job_config.parameters = {
        "azml": {
            "subscriptionId": "test-subscription-id",
            "resourceGroup": "test-resource-group",
            "mlWorkspaceName": "test-ml-workspace",
        }
    }
    return job_config


@pytest.fixture
def mock_vault_name():
    """Return a mock vault name"""
    return "test-vault"


@pytest.fixture
def azml_config(mock_job_config, mock_vault_name):
    """Create an AzMLConfig instance"""
    return AzMLConfig(mock_job_config, mock_vault_name)


class TestAzMLConfig:
    """Test cases for AzMLConfig class"""

    def test_init_success(self, mock_job_config, mock_vault_name):
        """Test successful initialization of AzMLConfig"""
        config = AzMLConfig(mock_job_config, mock_vault_name)

        assert config.subscription_id == "test-subscription-id"
        assert config.resource_group_name == "test-resource-group"
        assert config.workspace_name == "test-ml-workspace"
        assert config.vault_name == "test-vault"

    def test_init_with_different_values(self):
        """Test initialization with different configuration values"""
        job_config = Mock()
        job_config.parameters = {
            "azml": {
                "subscriptionId": "another-subscription",
                "resourceGroup": "another-rg",
                "mlWorkspaceName": "another-workspace",
            }
        }
        vault_name = "another-vault"

        config = AzMLConfig(job_config, vault_name)

        assert config.subscription_id == "another-subscription"
        assert config.resource_group_name == "another-rg"
        assert config.workspace_name == "another-workspace"
        assert config.vault_name == "another-vault"

    def test_init_missing_azml_config(self):
        """Test initialization fails when azml config is missing"""
        job_config = Mock()
        job_config.parameters = {}

        with pytest.raises(KeyError):
            AzMLConfig(job_config, "test-vault")

    def test_init_missing_subscription_id(self):
        """Test initialization fails when subscriptionId is missing"""
        job_config = Mock()
        job_config.parameters = {
            "azml": {
                "resourceGroup": "test-rg",
                "mlWorkspaceName": "test-workspace",
            }
        }

        with pytest.raises(KeyError):
            AzMLConfig(job_config, "test-vault")


class TestMlFlowUtils:
    """Test cases for MlFlowUtils class"""

    @patch("drift.tools.azml.MLClient")
    @patch("drift.tools.azml.ClientSecretCredential")
    @patch("drift.tools.azml.ServicePrincipalConfiguration")
    @patch("drift.tools.azml.dbutils", create=True)
    def test_init_success(
        self,
        mock_dbutils,
        mock_sp_config,
        mock_client_secret_credential,
        mock_ml_client,
        azml_config,
    ):
        """Test successful initialization of MlFlowUtils"""
        # Setup mocks
        mock_dbutils.secrets.get.side_effect = lambda scope, key: {
            "TenantID": "test-tenant-id",
            "ApplicationID": "test-app-id",
            "ApplicationPassword": "test-app-password",
        }[key]

        mock_workspace = Mock()
        mock_workspace.mlflow_tracking_uri = "test-tracking-uri"
        mock_ml_client_instance = Mock()
        mock_ml_client_instance.workspace_name = "test-workspace"
        mock_ml_client_instance.workspaces.get.return_value = mock_workspace
        mock_ml_client.return_value = mock_ml_client_instance

        mock_credential_instance = Mock()
        mock_client_secret_credential.return_value = mock_credential_instance

        mock_sp_config_instance = Mock()
        mock_sp_config.return_value = mock_sp_config_instance

        # Patch databricks module import
        with patch.dict("sys.modules", {"databricks.sdk.runtime": MagicMock(dbutils=mock_dbutils)}):
            utils = MlFlowUtils(azml_config)

            # Verify secrets were retrieved
            assert mock_dbutils.secrets.get.call_count == 3
            mock_dbutils.secrets.get.assert_any_call(scope="test-vault", key="TenantID")
            mock_dbutils.secrets.get.assert_any_call(scope="test-vault", key="ApplicationID")
            mock_dbutils.secrets.get.assert_any_call(scope="test-vault", key="ApplicationPassword")

            # Verify credential was created
            mock_client_secret_credential.assert_called_once_with(
                tenant_id="test-tenant-id",
                client_id="test-app-id",
                client_secret="test-app-password",
            )

            # Verify MLClient was created
            mock_ml_client.assert_called_once_with(
                credential=mock_credential_instance,
                subscription_id="test-subscription-id",
                resource_group_name="test-resource-group",
                workspace_name="test-ml-workspace",
            )

            # Verify ServicePrincipalConfiguration was created
            mock_sp_config.assert_called_once_with(
                tenant_id="test-tenant-id",
                client_id="test-app-id",
                client_secret="test-app-password",
            )

            # Verify environment variables were set
            assert os.environ["AZURE_TENANT_ID"] == "test-tenant-id"
            assert os.environ["AZURE_CLIENT_ID"] == "test-app-id"
            assert os.environ["AZURE_CLIENT_SECRET"] == "test-app-password"

            # Verify mlflow_tracking_uri was set
            assert utils.mlflow_tracking_uri == "test-tracking-uri"

            # Verify attributes
            assert utils.ml_client == mock_ml_client_instance
            assert utils.sp_config == mock_sp_config_instance

    def test_init_with_dbutils_import_error(self, azml_config):
        """Test initialization fails when databricks module cannot be imported"""
        # Simulate missing databricks module by raising ImportError on import
        import sys
        
        # Remove databricks modules if they exist
        modules_to_remove = [key for key in sys.modules.keys() if key.startswith("databricks")]
        for module in modules_to_remove:
            del sys.modules[module]
        
        # Mock the import to raise ImportError
        with patch.dict("sys.modules", {"databricks.sdk.runtime": None}):
            with pytest.raises((ImportError, ModuleNotFoundError, AttributeError)):
                MlFlowUtils(azml_config)

    @patch("drift.tools.azml.MLClient")
    @patch("drift.tools.azml.ClientSecretCredential")
    @patch("drift.tools.azml.ServicePrincipalConfiguration")
    @patch("drift.tools.azml.dbutils", create=True)
    def test_init_with_missing_secret(
        self,
        mock_dbutils,
        mock_sp_config,
        mock_client_secret_credential,
        mock_ml_client,
        azml_config,
    ):
        """Test initialization fails when a secret is missing"""
        mock_dbutils.secrets.get.side_effect = Exception("Secret not found")

        with patch.dict("sys.modules", {"databricks.sdk.runtime": MagicMock(dbutils=mock_dbutils)}):
            with pytest.raises(Exception, match="Secret not found"):
                MlFlowUtils(azml_config)


class TestInitMlFlowUtils:
    """Test cases for init_ml_flow_utils function"""

    @patch("drift.tools.azml.MlFlowUtils")
    @patch("drift.tools.azml.AzMLConfig")
    def test_init_ml_flow_utils_success(self, mock_azml_config_class, mock_mlflow_utils_class, mock_job_config, mock_vault_name):
        """Test successful initialization of MLFlow utils"""
        mock_azml_config_instance = Mock()
        mock_azml_config_class.return_value = mock_azml_config_instance

        mock_mlflow_utils_instance = Mock()
        mock_mlflow_utils_class.return_value = mock_mlflow_utils_instance

        result = init_ml_flow_utils(mock_job_config, mock_vault_name)

        # Verify AzMLConfig was created
        mock_azml_config_class.assert_called_once_with(mock_job_config, mock_vault_name)

        # Verify MlFlowUtils was created with AzMLConfig
        mock_mlflow_utils_class.assert_called_once_with(mock_azml_config_instance)

        # Verify result
        assert result == mock_mlflow_utils_instance

    @patch("drift.tools.azml.MlFlowUtils")
    @patch("drift.tools.azml.AzMLConfig")
    def test_init_ml_flow_utils_with_different_params(self, mock_azml_config_class, mock_mlflow_utils_class):
        """Test init_ml_flow_utils with different parameters"""
        job_config = Mock()
        job_config.parameters = {
            "azml": {
                "subscriptionId": "different-subscription",
                "resourceGroup": "different-rg",
                "mlWorkspaceName": "different-workspace",
            }
        }
        vault_name = "different-vault"

        mock_azml_config_instance = Mock()
        mock_azml_config_class.return_value = mock_azml_config_instance

        mock_mlflow_utils_instance = Mock()
        mock_mlflow_utils_class.return_value = mock_mlflow_utils_instance

        result = init_ml_flow_utils(job_config, vault_name)

        # Verify correct parameters were passed
        mock_azml_config_class.assert_called_once_with(job_config, vault_name)
        mock_mlflow_utils_class.assert_called_once_with(mock_azml_config_instance)
        assert result == mock_mlflow_utils_instance



