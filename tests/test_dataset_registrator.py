"""Tests for DatasetRegistrator"""
import re
from unittest.mock import Mock

from drift.registrating.dataset_registrator import DatasetRegistrator


def test_compute_version_format():
    """Test version format is YYYYMMDDHHMMSS and timestamp is ISO format"""
    version, timestamp = DatasetRegistrator.compute_version()
    
    # Version should be 14 digits (YYYYMMDDHHMMSS)
    assert re.match(r'^\d{14}$', version)
    
    # Timestamp should be ISO 8601 format
    assert 'T' in timestamp
    assert 'Z' in timestamp


def test_load_parameters_extracts_config():
    """Test parameter extraction from job config"""
    job_config = Mock()
    job_config.parameters = {
        "azml": {
            "subscriptionId": "test-sub",
            "resourceGroup": "test-rg",
            "mlWorkspaceName": "test-workspace"
        },
        "storageAccountName": "teststorage",
        "containerName": "testcontainer",
        "containerDataPath": "data/path"
    }
    
    params = DatasetRegistrator.load_parameters(job_config)
    
    assert params['subscription_id'] == "test-sub"
    assert params['resource_group'] == "test-rg"
    assert params['ml_workspace_name'] == "test-workspace"
    assert params['storage_account_name'] == "teststorage"
    assert params['container_name'] == "testcontainer"
    assert params['container_path'] == "data/path"
