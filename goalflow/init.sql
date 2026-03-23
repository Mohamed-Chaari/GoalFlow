CREATE TABLE IF NOT EXISTS matches (
    id SERIAL PRIMARY KEY,
    match_id INTEGER UNIQUE,
    home_team VARCHAR(100),
    away_team VARCHAR(100),
    home_score INTEGER,
    away_score INTEGER,
    league VARCHAR(100),
    country VARCHAR(100),
    match_date DATE,
    status VARCHAR(50),
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS standings (
    id SERIAL PRIMARY KEY,
    team VARCHAR(100),
    league VARCHAR(100),
    country VARCHAR(100),
    rank INTEGER,
    points INTEGER,
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    goals_for INTEGER,
    goals_against INTEGER,
    goal_diff INTEGER,
    season INTEGER,
    computed_date DATE,
    ingested_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS team_metrics (
    id SERIAL PRIMARY KEY,
    team VARCHAR(100),
    league VARCHAR(100),
    form_last5 VARCHAR(5),
    win_rate FLOAT,
    avg_goals_scored FLOAT,
    avg_goals_conceded FLOAT,
    total_matches INTEGER,
    computed_date DATE,
    created_at TIMESTAMP DEFAULT NOW()
);
