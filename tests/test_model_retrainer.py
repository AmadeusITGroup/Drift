"""Tests for ModelRetrainer"""
from unittest.mock import Mock

import pytest

from drift.retraining.model_retrainer import ModelRetrainer
from drift.retraining.job_group import JobGroup
from tests.conftest import create_mock_pipeline_job


@pytest.fixture
def model_retrainer(mock_job_config):
    """Create a ModelRetrainer instance"""
    retrainer = ModelRetrainer()
    retrainer.jobConfig = mock_job_config
    return retrainer


def test_job_name_pattern_with_prefix(model_retrainer):
    """Test job name pattern generation with model prefix"""
    model_retrainer.compute_jobname_pattern({"model_name_prefix": "modelA,modelB"})
    assert model_retrainer.job_name_pattern == r"^(modelA|modelB)_[0-9]{14}_.*$"


def test_is_in_scope_filters_correctly(model_retrainer):
    """Test job filtering by name pattern and data assets"""
    model_retrainer.job_name_pattern = r"^model_[0-9]{14}_.*$"
    
    # Should accept: matching name and assets
    valid_job = create_mock_pipeline_job("model_20231115120000_abc")
    assert model_retrainer.is_in_scope(valid_job) is True
    
    # Should reject: wrong name
    wrong_name = create_mock_pipeline_job("other_20231115120000_abc")
    assert model_retrainer.is_in_scope(wrong_name) is False
    
    # Should reject: wrong assets
    wrong_asset = create_mock_pipeline_job("model_20231115120000_abc", 
                                           train_path="azureml://wrong/path:v1")
    assert model_retrainer.is_in_scope(wrong_asset) is False


def test_retrieve_jobs_picks_newest_per_group(model_retrainer, mock_ml_client):
    """Test that only the newest job per group is selected for retraining"""
    model_retrainer.job_name_pattern = r"^model_[0-9]{14}_.*$"
    
    old_job = create_mock_pipeline_job("model_20231115120000_abc")
    new_job = create_mock_pipeline_job("model_20231115130000_def")
    
    mock_ml_client.jobs.list.return_value = [old_job, new_job]
    
    result = model_retrainer.retrieve_jobs_to_retrain(mock_ml_client)
    
    assert len(result) == 1
    assert result[0].training_timestamp == "20231115130000"


def test_retrieve_jobs_raises_when_none_found(model_retrainer, mock_ml_client):
    """Test error handling when no jobs match criteria"""
    model_retrainer.job_name_pattern = r"^model_[0-9]{14}_.*$"
    mock_ml_client.jobs.list.return_value = []
    
    with pytest.raises(Exception, match="No jobs in scope to retrain"):
        model_retrainer.retrieve_jobs_to_retrain(mock_ml_client)


def test_retrain_models_creates_new_jobs(model_retrainer, mock_ml_client):
    """Test end-to-end retraining flow"""
    job_template = create_mock_pipeline_job("model_20231115120000_abc")
    job_group = JobGroup("model", 20231115120000, job_template)
    
    mock_ml_client.jobs.create_or_update.return_value = create_mock_pipeline_job("model_20231115130000_new")
    model_retrainer.training_status_refresher = Mock()
    model_retrainer.training_status_refresher.wait_training.return_value = []
    
    result = model_retrainer.retrain_models(mock_ml_client, [job_group], {"data_asset_version": "v2"})
    
    assert len(result) == 1
    assert mock_ml_client.jobs.create_or_update.called


def test_check_success_raises_on_failures(model_retrainer):
    """Test that failed jobs cause an exception"""
    model_retrainer.training_status_refresher = Mock()
    model_retrainer.training_status_refresher.wait_training.return_value = [
        create_mock_pipeline_job("failed_job")
    ]
    
    with pytest.raises(Exception, match="Some jobs failed"):
        model_retrainer.check_success([create_mock_pipeline_job("job")])
