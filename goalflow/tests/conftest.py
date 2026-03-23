"""
GoalFlow Pytest Configuration
Author: Mohamed Chaari

Sets up test fixtures and mocks for pytest.
"""
import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_settings():
    """Returns a mock Settings object."""
    class MockSettings:
        football_api_key = "test_key"
        football_api_host = "test_host"
        kafka_bootstrap_servers = "test_servers"
        postgres_user = "test_user"
        postgres_password = "test_password"
        postgres_db = "test_db"
        postgres_host = "test_pg_host"
    return MockSettings()

@pytest.fixture
def mock_producer():
    """Returns a mocked Kafka Producer."""
    return MagicMock()

@pytest.fixture
def mock_requests_get(mocker):
    """Mocks requests.get."""
    return mocker.patch("requests.get")

@pytest.fixture
def mock_spark_session(mocker):
    """Mocks PySpark SparkSession and active context."""
    mock_spark = MagicMock()
    mock_builder = MagicMock()
    mocker.patch("pyspark.sql.SparkSession.builder", mock_builder)
    mocker.patch("pyspark.SparkContext._active_spark_context", MagicMock())
    mocker.patch("pyspark.sql.functions.col", MagicMock())
    mocker.patch("pyspark.sql.functions.from_json", MagicMock())
    mocker.patch("pyspark.sql.functions.current_date", MagicMock())
    mocker.patch("pyspark.sql.functions.current_timestamp", MagicMock())
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = mock_spark
    return mock_spark
