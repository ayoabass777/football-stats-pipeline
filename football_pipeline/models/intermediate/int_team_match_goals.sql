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

        home_team_id AS team_id,
        away_team_id AS opponent_id,
        'home' AS venue,
        home_team_fulltime_goal AS goals_scored,
        away_team_fulltime_goal AS goals_conceded
    FROM {{ ref('stg_raw__completed_fixtures') }}

    UNION ALL

    --- AWAY games
    SELECT
        fixture_id,
        date,
        season,
        league_id,
        country,
        league,

        away_team_id AS team_id,
        home_team_id AS opponent_id,
        'away' AS venue,
        home_team_fulltime_goal AS goals_conceded,
        away_team_fulltime_goal AS goals_scored
    FROM {{ ref('stg_raw__completed_fixtures') }}
)

SELECT * FROM base