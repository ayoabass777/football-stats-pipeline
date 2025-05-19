{{ config(materialized='table') }}

WITH base AS (
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
        t.venue AS venue,
        t.result AS result
    FROM{{ ref('stg_raw__completed_fixtures') }} as f
    CROSS JOIN LATERAL (
        VALUES 
        (f.home_team_id, f.away_team_id, f.home_team_name, f.away_team_name, f.home_fulltime_result, 'home'),
        (f.away_team_id, f.home_team_id, f.away_team_name, f.home_team_name, f.away_fulltime_result , 'away')
    ) AS t(team_id, opponent_id, team_name, opponent_name, result, venue)
    ORDER BY f.date, f.fixture_id, t.team_id 
)

SELECT * FROM base