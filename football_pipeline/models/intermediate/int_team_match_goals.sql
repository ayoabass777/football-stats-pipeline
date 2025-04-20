{{ config(materialized='table') }}

WITH base AS (
    --HOME games
    SELECT
        fixture_id,
        date,
        season,
        league_id,
        country,
        league,
        t.team_id AS team_id,
        t.opponent_id AS opponent_id,
        t.team_name AS team_name,
        t.opponent_name AS opponent_name,
        t.goals_scored AS goals_scored,
        t.goals_conceded AS goals_conceded,
        t.venue AS venue

    FROM {{ ref('stg_raw__completed_fixtures') }} AS f
    CROSS JOIN LATERAL (
        VALUES 
        (f.home_team_id, f.away_team_id, f.home_team_name, f.away_team_name, f.home_team_fulltime_goal, f.away_team_fulltime_goal, 'home'),
        (f.away_team_id, f.home_team_id, f.away_team_name, f.home_team_name, f.away_team_fulltime_goal, f.home_team_fulltime_goal, 'away')
    ) AS t(team_id, opponent_id, team_name, opponent_name, goals_scored, goals_conceded, venue)
    ORDER BY f.date, f.fixture_id, t.team_id 
)

SELECT * FROM base