"""
GoalFlow Spark Processor
Author: Mohamed Chaari

Reads raw football data from Kafka, processes it with PySpark, and writes to PostgreSQL.
"""
import sys
import os
import time
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_date, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pydantic_settings import BaseSettings
import pandera.pandas as pa
import psycopg2

from pydantic import ConfigDict

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"
    postgres_user: str = "goalflow"
    postgres_password: str = "goalflow123"
    postgres_db: str = "goalflow"
    postgres_host: str = "postgres"

    model_config = ConfigDict(env_file=".env", extra="ignore")

def create_spark_session() -> SparkSession:
    """Initializes and returns a PySpark session."""
    return SparkSession.builder \
        .appName("GoalFlow Processor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

def get_matches_schema() -> StructType:
    """Returns the schema for raw matches."""
    return StructType([
        StructField("match_id", IntegerType(), True),
        StructField("home_team", StringType(), True),
        StructField("away_team", StringType(), True),
        StructField("home_score", IntegerType(), True),
        StructField("away_score", IntegerType(), True),
        StructField("league", StringType(), True),
        StructField("country", StringType(), True),
        StructField("match_date", StringType(), True),
        StructField("status", StringType(), True)
    ])

def get_standings_schema() -> StructType:
    """Returns the schema for raw standings."""
    return StructType([
        StructField("team", StringType(), True),
        StructField("league", StringType(), True),
        StructField("country", StringType(), True),
        StructField("rank", IntegerType(), True),
        StructField("points", IntegerType(), True),
        StructField("wins", IntegerType(), True),
        StructField("draws", IntegerType(), True),
        StructField("losses", IntegerType(), True),
        StructField("goals_for", IntegerType(), True),
        StructField("goals_against", IntegerType(), True),
        StructField("goal_diff", IntegerType(), True),
        StructField("season", IntegerType(), True),
        StructField("form", StringType(), True)
    ])

# Pandera schemas for validation
matches_pandera_schema = pa.DataFrameSchema({
    "match_id": pa.Column(int, required=True),
    "home_team": pa.Column(str, required=True),
    "away_team": pa.Column(str, required=True),
    "home_score": pa.Column(int, required=True, nullable=True),
    "away_score": pa.Column(int, required=True, nullable=True),
    "league": pa.Column(str, required=True),
    "country": pa.Column(str, required=True),
    "match_date": pa.Column(str, required=True),
    "status": pa.Column(str, required=True)
})

standings_pandera_schema = pa.DataFrameSchema({
    "team": pa.Column(str, required=True),
    "league": pa.Column(str, required=True),
    "country": pa.Column(str, required=True),
    "rank": pa.Column(int, required=True),
    "points": pa.Column(int, required=True),
    "wins": pa.Column(int, required=True),
    "draws": pa.Column(int, required=True),
    "losses": pa.Column(int, required=True),
    "goals_for": pa.Column(int, required=True),
    "goals_against": pa.Column(int, required=True),
    "goal_diff": pa.Column(int, required=True),
    "season": pa.Column(int, required=True),
    "form": pa.Column(str, required=False, nullable=True)
})

def validate_dataframe(df, schema, name: str):
    """Validates a pandas DataFrame using Pandera."""
    try:
        schema.validate(df)
        logger.info(f"Pandera validation passed for {name}")
    except pa.errors.SchemaError as err:
        logger.error(f"Pandera validation failed for {name}: {err}")
        raise

def write_to_postgres(df, table_name: str, settings: Settings, mode: str = "append"):
    """Writes a Spark DataFrame to PostgreSQL using JDBC."""
    url = f"jdbc:postgresql://{settings.postgres_host}:5432/{settings.postgres_db}"
    properties = {
        "user": settings.postgres_user,
        "password": settings.postgres_password,
        "driver": "org.postgresql.Driver"
    }

    count = df.count()
    logger.info(f"Writing {count} rows to PostgreSQL table {table_name}")

    if count == 0:
        return

    if mode == "upsert" and table_name == "matches":
        temp_table = f"{table_name}_staging"
        df.write.jdbc(url=url, table=temp_table, mode="overwrite", properties=properties)

        conn = psycopg2.connect(
            dbname=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
            host=settings.postgres_host
        )
        cur = conn.cursor()

        # PostgreSQL syntax for upsert
        upsert_query = f"""
            INSERT INTO {table_name} (match_id, home_team, away_team, home_score, away_score, league, country, match_date, status, ingested_at)
            SELECT match_id, home_team, away_team, home_score, away_score, league, country, CAST(match_date AS DATE), status, current_timestamp
            FROM {temp_table}
            ON CONFLICT (match_id) DO UPDATE SET
                home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                status = EXCLUDED.status,
                ingested_at = current_timestamp;
        """
        cur.execute(upsert_query)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Upserted {count} matches into {table_name}")
    else:
        df.write.jdbc(url=url, table=table_name, mode=mode, properties=properties)
        logger.info(f"Appended {count} rows to {table_name}")

def process_matches(spark: SparkSession, settings: Settings):
    """Reads matches from Kafka, processes them, and writes to PostgreSQL."""
    logger.info("Processing matches from Kafka")

    try:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
            .option("subscribe", "matches-raw") \
            .option("startingOffsets", "earliest") \
            .load()
    except Exception as e:
        logger.warning(f"Failed to read matches from Kafka. Topic might not exist yet: {e}")
        return

    if df.isEmpty():
        logger.info("No messages in matches-raw topic.")
        return

    # Parse JSON payload
    parsed_df = df.select(from_json(col("value").cast("string"), get_matches_schema()).alias("data")).select("data.*")

    # Pandera Validation (converting to pandas first, as pandera works on pandas)
    # For large datasets, pandera-pyspark or skipping pandera is better,
    # but for small batch size (30 rows), converting to pandas is fine.
    pdf = parsed_df.toPandas()
    validate_dataframe(pdf, matches_pandera_schema, "matches-raw")

    # Deduplicate in case of multiple runs
    dedup_df = parsed_df.dropDuplicates(["match_id"])

    # Write to matches table (upsert)
    write_to_postgres(dedup_df, "matches", settings, mode="upsert")

def process_standings_and_metrics(spark: SparkSession, settings: Settings):
    """Reads standings from Kafka, computes metrics, and writes to PostgreSQL."""
    logger.info("Processing standings from Kafka")

    try:
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers) \
            .option("subscribe", "standings-raw") \
            .option("startingOffsets", "earliest") \
            .load()
    except Exception as e:
        logger.warning(f"Failed to read standings from Kafka. Topic might not exist yet: {e}")
        return

    if df.isEmpty():
        logger.info("No messages in standings-raw topic.")
        return

    # Parse JSON payload
    parsed_df = df.select(from_json(col("value").cast("string"), get_standings_schema()).alias("data")).select("data.*")

    # Pandera Validation
    pdf = parsed_df.toPandas()
    validate_dataframe(pdf, standings_pandera_schema, "standings-raw")

    # Add computed date and ingested_at
    enriched_df = parsed_df.withColumn("computed_date", current_date()) \
                           .withColumn("ingested_at", current_timestamp())

    # Write to standings table (append)
    write_to_postgres(enriched_df, "standings", settings, mode="append")

    # Compute team metrics
    metrics_df = parsed_df.withColumn("total_matches", col("wins") + col("draws") + col("losses")) \
        .withColumn("win_rate", col("wins") / col("total_matches")) \
        .withColumn("avg_goals_scored", col("goals_for") / col("total_matches")) \
        .withColumn("avg_goals_conceded", col("goals_against") / col("total_matches")) \
        .withColumnRenamed("form", "form_last5") \
        .withColumn("computed_date", current_date()) \
        .withColumn("created_at", current_timestamp()) \
        .select("team", "league", "form_last5", "win_rate", "avg_goals_scored", "avg_goals_conceded", "total_matches", "computed_date", "created_at")

    # Write to team_metrics table (append)
    write_to_postgres(metrics_df, "team_metrics", settings, mode="append")

def main():
    """Main execution function for the Spark processor."""
    logger.info("Starting Spark processing job")
    settings = Settings()

    # Small delay to ensure Kafka is fully ready when run from docker
    time.sleep(5)

    spark = create_spark_session()

    process_matches(spark, settings)
    process_standings_and_metrics(spark, settings)

    spark.stop()
    logger.info("Spark processing job completed successfully")

if __name__ == "__main__":
    main()
