"""Tests for DataAssetRegistrator"""
from unittest.mock import Mock

from drift.registrating.data_asset_registrator import DataAssetRegistrator


def test_init_computes_names():
    """Test mltable_name and data_asset_uri generation from container path"""
    ml_client = Mock()
    sp_config = Mock()
    parameters = {
        "subscription_id": "test-sub",
        "resource_group": "test-rg",
        "ml_workspace_name": "test-workspace",
        "storage_account_name": "teststorage",
        "container_name": "test-container",
        "container_path": "data/training/path"
    }
    
    registrator = DataAssetRegistrator(ml_client, sp_config, parameters, "v1", "2023-11-15T12:00:00Z")
    
    # Should contain container name and sanitized path
    assert "test-container" in registrator.mltable_name
    assert "data-training-path" in registrator.mltable_name
    assert "mltable" in registrator.mltable_name
    
    assert "test-container" in registrator.data_asset_uri
    assert "data-training-path" in registrator.data_asset_uri
    assert "uri" in registrator.data_asset_uri


def test_path_sanitization():
    """Test that forward slashes in paths are replaced with hyphens"""
    ml_client = Mock()
    sp_config = Mock()
    parameters = {
        "subscription_id": "test-sub",
        "resource_group": "test-rg",
        "ml_workspace_name": "test-workspace",
        "storage_account_name": "teststorage",
        "container_name": "container",
        "container_path": "/leading/middle/trailing/"
    }
    
    registrator = DataAssetRegistrator(ml_client, sp_config, parameters, "v1", "2023-11-15T12:00:00Z")
    
    # Should strip leading/trailing slashes and replace middle ones with hyphens
    assert registrator.mltable_name == "container-leading-middle-trailing-mltable"
    assert registrator.data_asset_uri == "container-leading-middle-trailing-uri"
