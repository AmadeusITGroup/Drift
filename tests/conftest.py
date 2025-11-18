"""Shared pytest fixtures and utilities for all tests"""
import logging
from unittest.mock import Mock

import pytest
from azure.ai.ml.entities import PipelineJob


@pytest.fixture
def mock_job_config():
    """Create a mock JobConfig object with common parameters"""
    job_config = Mock()
    job_config.parameters = {
        "dataAssets": [
            {"name": "training_data", "value": "azureml://datastores/data/paths/train"},
            {"name": "validation_data", "value": "azureml://datastores/data/paths/val"},
        ],
        "refreshTimeout": "3600",
        "refreshDelay": "30",
        "azml": {
            "subscriptionId": "test-subscription-id",
            "resourceGroup": "test-resource-group",
            "mlWorkspaceName": "test-ml-workspace",
        },
    }
    return job_config


@pytest.fixture
def mock_ml_client():
    """Create a mock MLClient object"""
    return Mock()


def create_mock_pipeline_job(display_name, train_path="azureml://datastores/data/paths/train:v1", 
                              val_path="azureml://datastores/data/paths/val:v1", 
                              name=None, status="Running"):
    """Helper to create a mock PipelineJob with common attributes"""
    job = Mock(spec=PipelineJob)
    job.name = name or display_name.replace(" ", "_").lower()
    job.display_name = display_name
    job.status = status
    job.inputs = {
        "training_data": Mock(path=train_path),
        "validation_data": Mock(path=val_path),
    }
    return job


def assert_logger_configured(module, expected_name):
    """Common assertion for logger configuration"""
    assert hasattr(module, "logger")
    assert isinstance(module.logger, logging.Logger)
    assert module.logger.name == expected_name
