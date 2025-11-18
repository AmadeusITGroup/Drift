"""Shared logging tests for all modules"""
import pytest

from tests.conftest import assert_logger_configured


@pytest.mark.parametrize("module_path,expected_name", [
    ("drift.tools.azml", "drift.tools.azml"),
    ("drift.retraining.model_retrainer", "drift.retraining.model_retrainer"),
    ("drift.retraining.training_status_refresher", "drift.retraining.training_status_refresher"),
])
def test_logger_configured(module_path, expected_name):
    """Test that all modules have properly configured loggers"""
    import importlib
    module = importlib.import_module(module_path)
    assert_logger_configured(module, expected_name)
