{{ config(materialized='view') }}

SELECT
    fixture_id,
    date,
    season,
    country,
    league,
    league_id,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    home_team_fulltime_goal,
    away_team_fulltime_goal,
    home_team_halftime_goal,
    away_team_halftime_goal,
    home_fulltime_result,
    away_fulltime_result,
    home_halftime_result,
    away_halftime_result,
    status

FROM {{ source('raw', 'raw_fixtures') }}
WHERE status = 'FT'