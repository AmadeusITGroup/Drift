"""Tests for TrainingStatusRefresher"""
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from drift.retraining.training_status_refresher import TrainingStatusRefresher
from tests.conftest import create_mock_pipeline_job


@pytest.fixture
def refresher(mock_job_config, mock_ml_client):
    """Create a TrainingStatusRefresher instance"""
    return TrainingStatusRefresher(mock_job_config, mock_ml_client)


def test_init_extracts_config(mock_job_config, mock_ml_client):
    """Test initialization extracts timeout and delay from config"""
    refresher = TrainingStatusRefresher(mock_job_config, mock_ml_client)
    
    assert refresher.timeout_delay == 3600
    assert refresher.refresh_delay == 30


def test_refresh_filters_completed_jobs(refresher):
    """Test that completed jobs are filtered out, others remain"""
    job1 = create_mock_pipeline_job("Job 1", name="job1")
    job2 = create_mock_pipeline_job("Job 2", name="job2")
    
    refresher.ml_client.jobs.get.side_effect = [
        create_mock_pipeline_job("Job 1", name="job1", status="Completed"),
        create_mock_pipeline_job("Job 2", name="job2", status="Running")
    ]
    
    result = refresher.refresh_job_status([job1, job2])
    
    assert len(result) == 1
    assert result[0].status == "Running"


def test_timeout_raises_exception(refresher):
    """Test that exceeding timeout raises an exception"""
    past_time = datetime.now() - timedelta(seconds=1)
    
    with pytest.raises(Exception, match="Timeout reached"):
        refresher.check_timeout_reached(past_time)


@patch("drift.retraining.training_status_refresher.time.sleep")
def test_wait_training_polls_until_done(mock_sleep, refresher):
    """Test wait_training polls jobs until all complete or fail"""
    job = create_mock_pipeline_job("Job 1", name="job1")
    
    # First poll: Running, Second poll: Completed
    refresher.ml_client.jobs.get.side_effect = [
        create_mock_pipeline_job("Job 1", name="job1", status="Running"),
        create_mock_pipeline_job("Job 1", name="job1", status="Completed")
    ]
    
    result = refresher.wait_training([job])
    
    assert len(result) == 0  # No failed jobs
    assert mock_sleep.call_count == 2


@patch("drift.retraining.training_status_refresher.time.sleep")
def test_wait_training_returns_failed_jobs(mock_sleep, refresher):
    """Test that failed jobs are returned"""
    job = create_mock_pipeline_job("Job 1", name="job1")
    refresher.ml_client.jobs.get.return_value = create_mock_pipeline_job("Job 1", name="job1", status="Failed")
    
    result = refresher.wait_training([job])
    
    assert len(result) == 1
    assert result[0].status == "Failed"
