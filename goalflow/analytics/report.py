"""
GoalFlow Analytics Report Generator
Author: Mohamed Chaari

Reads processed data from PostgreSQL and generates an HTML report with Plotly.
"""
import os
import datetime
from loguru import logger
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    postgres_user: str = "goalflow"
    postgres_password: str = "goalflow123"
    postgres_db: str = "goalflow"
    postgres_host: str = "postgres"

    class Config:
        env_file = ".env"

def get_db_connection(settings: Settings):
    """Establishes and returns a connection to PostgreSQL."""
    return psycopg2.connect(
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        host=settings.postgres_host,
        port=5432
    )

def fetch_data(conn) -> pd.DataFrame:
    """Fetches data from PostgreSQL for analytics."""
    logger.info("Fetching data from PostgreSQL")

    # We want latest standings
    standings_query = """
        SELECT team, league, country, points, rank
        FROM standings
        WHERE computed_date = (SELECT MAX(computed_date) FROM standings)
        ORDER BY points DESC
    """

    # We want latest team metrics
    metrics_query = """
        SELECT team, league, form_last5, avg_goals_scored, avg_goals_conceded
        FROM team_metrics
        WHERE computed_date = (SELECT MAX(computed_date) FROM team_metrics)
    """

    try:
        standings_df = pd.read_sql_query(standings_query, conn)
        metrics_df = pd.read_sql_query(metrics_query, conn)
        return standings_df, metrics_df
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        return pd.DataFrame(), pd.DataFrame()

def generate_league_table_chart(df: pd.DataFrame) -> str:
    """Generates a horizontal bar chart of teams ranked by points."""
    if df.empty:
        return "<p>No standings data available.</p>"

    logger.info("Generating League Table Chart")

    fig = px.bar(
        df,
        x='points',
        y='team',
        color='league',
        orientation='h',
        title="Current League Standings (by Points)",
        labels={'points': 'Points', 'team': 'Team'},
        hover_data=['rank', 'country']
    )

    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    return fig.to_html(full_html=False, include_plotlyjs=False)

def generate_top_scorers_chart(df: pd.DataFrame) -> str:
    """Generates a bar chart of top 10 teams by average goals scored."""
    if df.empty:
        return "<p>No metrics data available.</p>"

    logger.info("Generating Top Scorers Chart")

    top_10 = df.nlargest(10, 'avg_goals_scored')

    fig = px.bar(
        top_10,
        x='team',
        y='avg_goals_scored',
        color='league',
        title="Top 10 Teams by Average Goals Scored",
        labels={'avg_goals_scored': 'Average Goals Scored', 'team': 'Team'}
    )

    return fig.to_html(full_html=False, include_plotlyjs=False)

def encode_form(form_str: str) -> list:
    """Encodes form string (e.g., 'WWDL') into numerical values for heatmap."""
    encoding = {'W': 3, 'D': 1, 'L': 0}
    # ensure it's a string and handle None
    if not isinstance(form_str, str):
        return [0] * 5

    encoded = []
    # Reverse to show earliest to latest, or just take as is
    # We'll take up to 5 chars
    for char in form_str[:5]:
        encoded.append(encoding.get(char, 0))

    # Pad if less than 5
    while len(encoded) < 5:
        encoded.append(0)

    return encoded

def generate_form_heatmap(df: pd.DataFrame) -> str:
    """Generates a heatmap of team forms over the last 5 matches."""
    if df.empty or 'form_last5' not in df.columns:
        return "<p>No form data available.</p>"

    logger.info("Generating Form Heatmap")

    # Take top teams to avoid massive heatmap, e.g., top 15 by avg_goals_scored
    top_teams = df.nlargest(15, 'avg_goals_scored')

    teams = top_teams['team'].tolist()
    forms = top_teams['form_last5'].tolist()

    z_data = []
    text_data = []

    for form in forms:
        encoded = encode_form(form)
        z_data.append(encoded)

        # Text to display on hover/heatmap
        text_row = []
        if isinstance(form, str):
            for char in form[:5]:
                text_row.append(char)
            while len(text_row) < 5:
                text_row.append("-")
        else:
            text_row = ["-"] * 5

        text_data.append(text_row)

    fig = go.Figure(data=go.Heatmap(
        z=z_data,
        x=['Match -4', 'Match -3', 'Match -2', 'Match -1', 'Latest'],
        y=teams,
        text=text_data,
        texttemplate="%{text}",
        colorscale=[[0, 'red'], [0.5, 'yellow'], [1, 'green']],
        showscale=False
    ))

    fig.update_layout(title="Form Heatmap (Top 15 Scoring Teams)")
    return fig.to_html(full_html=False, include_plotlyjs=False)

def main():
    """Main execution function for generating the analytics report."""
    logger.info("Starting report generation")
    settings = Settings()

    try:
        conn = get_db_connection(settings)
    except psycopg2.OperationalError as e:
        logger.error(f"Failed to connect to DB: {e}. Exiting.")
        return

    standings_df, metrics_df = fetch_data(conn)
    conn.close()

    league_chart_html = generate_league_table_chart(standings_df)
    scorers_chart_html = generate_top_scorers_chart(metrics_df)
    heatmap_html = generate_form_heatmap(metrics_df)

    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>GoalFlow Analytics Report</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .container {{ max-width: 1200px; margin: auto; }}
            .chart {{ margin-bottom: 50px; border: 1px solid #ccc; padding: 10px; border-radius: 5px; }}
            h1 {{ color: #333; }}
            p.meta {{ color: #666; font-style: italic; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>GoalFlow Analytics Report</h1>
            <p class="meta">Generated: {now}</p>

            <div class="chart">
                {league_chart_html}
            </div>

            <div class="chart">
                {scorers_chart_html}
            </div>

            <div class="chart">
                {heatmap_html}
            </div>
        </div>
    </body>
    </html>
    """

    # Ensure output directory exists
    os.makedirs("/app/output", exist_ok=True)

    output_path = "/app/output/report.html"
    with open(output_path, "w") as f:
        f.write(html_content)

    logger.info(f"Report successfully generated at {output_path}")

if __name__ == "__main__":
    main()
