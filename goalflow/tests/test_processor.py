"""
GoalFlow Processor Tests
Author: Mohamed Chaari

Tests for spark/processor.py.
"""
import pytest
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.append(os.path.join(os.path.dirname(__file__), '../spark'))
from processor import process_matches, process_standings_and_metrics, Settings

def test_process_matches_empty_df(mock_spark_session):
    """Test process_matches when Kafka topic is empty."""
    settings = Settings(kafka_bootstrap_servers="test", postgres_user="u", postgres_password="p", postgres_db="db", postgres_host="h")

    mock_df = MagicMock()
    mock_df.isEmpty.return_value = True

    mock_read = MagicMock()
    mock_read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    mock_spark_session.read = mock_read

    process_matches(mock_spark_session, settings)

    mock_df.select.assert_not_called()

@patch("processor.write_to_postgres")
@patch("processor.validate_dataframe")
def test_process_matches_with_data(mock_validate, mock_write, mock_spark_session):
    """Test process_matches handles data correctly."""
    settings = Settings(kafka_bootstrap_servers="test", postgres_user="u", postgres_password="p", postgres_db="db", postgres_host="h")

    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False

    mock_parsed_df = MagicMock()
    mock_df.select.return_value.select.return_value = mock_parsed_df

    mock_read = MagicMock()
    mock_read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    mock_spark_session.read = mock_read

    mock_dedup_df = MagicMock()
    mock_parsed_df.dropDuplicates.return_value = mock_dedup_df

    process_matches(mock_spark_session, settings)

    mock_parsed_df.toPandas.assert_called_once()
    mock_validate.assert_called_once()
    mock_write.assert_called_once_with(mock_dedup_df, "matches", settings, mode="upsert")

@patch("processor.write_to_postgres")
@patch("processor.validate_dataframe")
def test_process_standings_and_metrics_with_data(mock_validate, mock_write, mock_spark_session):
    """Test process_standings_and_metrics handles data correctly."""
    settings = Settings(kafka_bootstrap_servers="test", postgres_user="u", postgres_password="p", postgres_db="db", postgres_host="h")

    mock_df = MagicMock()
    mock_df.isEmpty.return_value = False

    mock_parsed_df = MagicMock()
    mock_df.select.return_value.select.return_value = mock_parsed_df

    mock_read = MagicMock()
    mock_read.format.return_value.option.return_value.option.return_value.option.return_value.load.return_value = mock_df
    mock_spark_session.read = mock_read

    mock_enriched_df = MagicMock()
    mock_parsed_df.withColumn.return_value.withColumn.return_value = mock_enriched_df

    mock_metrics_df = MagicMock()
    mock_parsed_df.withColumn.return_value.withColumn.return_value.withColumn.return_value.withColumn.return_value.withColumnRenamed.return_value.withColumn.return_value.withColumn.return_value.select.return_value = mock_metrics_df

    process_standings_and_metrics(mock_spark_session, settings)

    mock_parsed_df.toPandas.assert_called_once()
    mock_validate.assert_called_once()
    assert mock_write.call_count == 2
