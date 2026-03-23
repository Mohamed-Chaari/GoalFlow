"""
GoalFlow Ingestion Producer
Author: Mohamed Chaari

Fetches football data from api-sports.io and produces to Kafka.
"""
import json
import time
from typing import Dict, Any, List
import requests
from loguru import logger
from confluent_kafka import Producer
from pydantic import BaseModel
from pydantic_settings import BaseSettings

from pydantic import ConfigDict

class Settings(BaseSettings):
    football_api_key: str
    football_api_host: str = "v3.football.api-sports.io"
    kafka_bootstrap_servers: str = "kafka:9092"

    model_config = ConfigDict(env_file=".env", extra="ignore")

class MatchRecord(BaseModel):
    match_id: int
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    league: str
    country: str
    match_date: str
    status: str

def get_producer(servers: str) -> Producer:
    """Initializes and returns a Kafka producer."""
    conf = {
        'bootstrap.servers': servers,
        'acks': 'all',
        'retries': 3
    }
    return Producer(conf)

def fetch_with_retry(url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Dict[str, Any]:
    """Fetches data from an API with exponential backoff for 5xx errors."""
    max_retries = 3
    base_delay = 2

    for attempt in range(max_retries + 1):
        try:
            logger.info(f"Fetching {url} with params {params} (Attempt {attempt + 1})")
            response = requests.get(url, headers=headers, params=params, timeout=10)

            if response.status_code == 429:
                logger.warning(f"Rate limit exceeded (429) for {url}. Skipping this run.")
                return {}

            response.raise_for_status()
            data = response.json()

            # api-sports.io might return errors in the response body even with 200 OK
            if data.get("errors"):
                # if errors is a dict and it has rate limit issues
                errors = data.get("errors")
                if isinstance(errors, dict) and any("rate limit" in str(v).lower() for v in errors.values()):
                    logger.warning(f"API returned rate limit error in body: {errors}. Skipping.")
                    return {}
                logger.error(f"API returned errors: {errors}")

            return data

        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 429:
                    logger.warning(f"Rate limit exceeded (429). Skipping.")
                    return {}
                if 400 <= e.response.status_code < 500:
                    logger.error(f"Client error {e.response.status_code}: {e}")
                    return {} # don't retry 4xx errors other than 429

            if attempt < max_retries:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Request failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"Max retries reached. Failed to fetch from {url}")
                raise

def fetch_and_produce_matches(settings: Settings, producer: Producer, league_id: int, season: int):
    """Fetches last 10 finished matches for a league and produces them to Kafka."""
    url = f"https://{settings.football_api_host}/fixtures"
    headers = {
        "x-apisports-key": settings.football_api_key,
        "x-rapidapi-host": settings.football_api_host
    }
    params = {
        "league": league_id,
        "season": season,
        "last": 10,
        "status": "FT"
    }

    data = fetch_with_retry(url, headers, params)
    if not data or not data.get("response"):
        return

    for item in data["response"]:
        fixture = item["fixture"]
        league_info = item["league"]
        teams = item["teams"]
        goals = item["goals"]

        match = MatchRecord(
            match_id=fixture["id"],
            home_team=teams["home"]["name"],
            away_team=teams["away"]["name"],
            home_score=goals["home"] if goals["home"] is not None else 0,
            away_score=goals["away"] if goals["away"] is not None else 0,
            league=league_info["name"],
            country=league_info["country"],
            match_date=fixture["date"].split("T")[0],
            status=fixture["status"]["short"]
        )

        payload = match.model_dump_json()
        producer.produce("matches-raw", key=str(match.match_id), value=payload)
        logger.info(f"Produced match: {match.home_team} vs {match.away_team} to matches-raw")

    producer.flush()

def fetch_and_produce_standings(settings: Settings, producer: Producer, league_id: int, season: int):
    """Fetches current standings for a league and produces them to Kafka."""
    url = f"https://{settings.football_api_host}/standings"
    headers = {
        "x-apisports-key": settings.football_api_key,
        "x-rapidapi-host": settings.football_api_host
    }
    params = {
        "league": league_id,
        "season": season
    }

    data = fetch_with_retry(url, headers, params)
    if not data or not data.get("response"):
        return

    league_data = data["response"][0]["league"]
    league_name = league_data["name"]
    country = league_data["country"]

    # Sometimes standings is an array of arrays (groups)
    standings_lists = league_data.get("standings", [])
    if not standings_lists:
        return

    # Assume first list is the main standings table
    for row in standings_lists[0]:
        team_data = {
            "team": row["team"]["name"],
            "league": league_name,
            "country": country,
            "rank": row["rank"],
            "points": row["points"],
            "wins": row["all"]["win"],
            "draws": row["all"]["draw"],
            "losses": row["all"]["lose"],
            "goals_for": row["all"]["goals"]["for"],
            "goals_against": row["all"]["goals"]["against"],
            "goal_diff": row["goalsDiff"],
            "season": season,
            "form": row.get("form", "")
        }

        # Produce as generic JSON
        payload = json.dumps(team_data)
        producer.produce("standings-raw", key=team_data["team"], value=payload)
        logger.info(f"Produced standings for: {team_data['team']} to standings-raw")

    producer.flush()

def main():
    """Main execution function for the producer."""
    logger.info("Starting ingestion producer")
    settings = Settings()
    producer = get_producer(settings.kafka_bootstrap_servers)

    leagues = [39, 140, 135] # Premier League, La Liga, Serie A
    season = 2024

    for league_id in leagues:
        logger.info(f"Processing league ID: {league_id}")
        fetch_and_produce_matches(settings, producer, league_id, season)
        fetch_and_produce_standings(settings, producer, league_id, season)

    logger.info("Ingestion completed successfully")

if __name__ == "__main__":
    main()
