"""
GoalFlow Producer Tests
Author: Mohamed Chaari

Tests for ingestion/producer.py.
"""
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../ingestion'))

from unittest.mock import MagicMock
from producer import fetch_with_retry, fetch_and_produce_matches, fetch_and_produce_standings

def test_fetch_with_retry_success(mock_requests_get):
    """Test API fetching when response is 200 OK."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"response": [{"id": 1}]}
    mock_requests_get.return_value = mock_response

    result = fetch_with_retry("http://test.com", {"key": "value"}, {"param": "1"})

    assert result == {"response": [{"id": 1}]}
    mock_requests_get.assert_called_once()

def test_fetch_with_retry_429(mock_requests_get):
    """Test API fetching when rate limited (429)."""
    mock_response = MagicMock()
    mock_response.status_code = 429
    mock_response.raise_for_status.side_effect = Exception("429")
    mock_requests_get.return_value = mock_response

    result = fetch_with_retry("http://test.com", {}, {})

    # Should skip gracefully
    assert result == {}
    mock_requests_get.assert_called_once()

def test_fetch_and_produce_matches(mock_settings, mock_producer, mock_requests_get):
    """Test fetching and producing matches to Kafka."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "response": [{
            "fixture": {"id": 123, "date": "2024-01-01T15:00:00+00:00", "status": {"short": "FT"}},
            "league": {"name": "Premier League", "country": "England"},
            "teams": {"home": {"name": "Arsenal"}, "away": {"name": "Chelsea"}},
            "goals": {"home": 2, "away": 1}
        }]
    }
    mock_requests_get.return_value = mock_response

    fetch_and_produce_matches(mock_settings, mock_producer, 39, 2024)

    assert mock_producer.produce.call_count == 1
    args, kwargs = mock_producer.produce.call_args
    assert args[0] == "matches-raw"
    assert kwargs["key"] == "123"
    assert "Arsenal" in kwargs["value"]
    mock_producer.flush.assert_called_once()

def test_fetch_and_produce_standings(mock_settings, mock_producer, mock_requests_get):
    """Test fetching and producing standings to Kafka."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "response": [{
            "league": {
                "name": "Premier League",
                "country": "England",
                "standings": [[
                    {
                        "team": {"name": "Arsenal"},
                        "rank": 1,
                        "points": 80,
                        "all": {
                            "win": 25,
                            "draw": 5,
                            "lose": 2,
                            "goals": {"for": 80, "against": 20}
                        },
                        "goalsDiff": 60,
                        "form": "WWWWW"
                    }
                ]]
            }
        }]
    }
    mock_requests_get.return_value = mock_response

    fetch_and_produce_standings(mock_settings, mock_producer, 39, 2024)

    assert mock_producer.produce.call_count == 1
    args, kwargs = mock_producer.produce.call_args
    assert args[0] == "standings-raw"
    assert kwargs["key"] == "Arsenal"
    assert "80" in kwargs["value"]
    mock_producer.flush.assert_called_once()
