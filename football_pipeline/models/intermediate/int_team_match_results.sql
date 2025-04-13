{{ config(materialized='table') }}

WITH base AS (
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
        home_fulltime_result AS result
    FROM {{ ref('stg_raw__completed_fixtures') }}

    UNION ALL

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
        away_fulltime_result AS result
    FROM{{ ref('stg_raw__completed_fixtures') }}
)

SELECT * FROM base