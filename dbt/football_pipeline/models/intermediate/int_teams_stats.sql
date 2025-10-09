{{ config(materialized='table')}}

-- fix variables using the CTE from fact_team_events####
WITH base AS(
    SELECT
        fixture_id AS fixture_id,
        matchday AS matchday,
        date AS date,
        season AS season,
        league_id AS league_id,
        country AS country,
        league AS league, 
        team_id AS team_id,
        team_name AS team_name,
        opponent_id AS opponent_id,
        opponent_name AS opponent_name,
        venue AS venue,
        result AS result,
        goals_for AS goals_scored,
        goals_against AS goals_conceded
    FROM {{ ref('stg_raw__completed_fixtures') }}
),


games_played_stats AS (
    SELECT *,
        COUNT(*) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS games_played,
        SUM(CASE WHEN result ='win' THEN 1 ELSE 0 END) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS wins,
        SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS losses,
        SUM(CASE WHEN result='draw' THEN 1 ELSE 0 END) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS draws,
        --goals for is sum of goal scored 
        SUM(goals_scored) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS goals_for,
        --goals against is sum of goal conceded
        SUM(goals_conceded) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS goals_against
    FROM base
),

rolling_rates_and_averages AS (
    SELECT *,
        ROUND((wins:: FLOAT / games_played)::numeric, 2) AS rolling_win_rate,
        ROUND((losses:: FLOAT / games_played)::numeric, 2) AS rolling_loss_rate,
        ROUND((draws:: FLOAT / games_played)::numeric, 2) AS rolling_draw_rate,
        ROUND(AVG(goals_scored)
            OVER(PARTITION BY league_id, season, team_id 
                ORDER BY date)::numeric, 2)AS rolling_AVG_goals_for,
        ROUND(AVG(goals_conceded) 
            OVER(PARTITION BY league_id, season, team_id 
                ORDER BY date)::numeric, 2) AS rolling_AVG_goals_against
    FROM games_played_stats
),

rolling_pts AS (
    SELECT *,
        SUM(CASE WHEN result ='win' THEN 3
                WHEN result='draw' THEN 1
                ELSE 0
            END) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS rolling_points
    FROM rolling_rates_and_averages
),

aggregator_CTE AS (
    SELECT *,
    rolling_win_rate + rolling_draw_rate  AS aggregator 
    FROM rolling_pts
),

final AS(
    SELECT *,
    DENSE_RANK() OVER(PARTITION BY league_id, season, matchday ORDER BY rolling_points DESC, aggregator DESC ) AS position
    FROM aggregator_CTE
)

SELECT
    date,
    matchday,
    team_name,
    team_id,
    position,
    league,
    league_id,
    country,
    season,
    result,
    opponent_name,
    opponent_id,
    games_played,
    wins,
    losses,
    draws,
    goals_scored,
    goals_conceded,
    goals_for,
    goals_against,
    rolling_win_rate,
    rolling_draw_rate,
    rolling_loss_rate,
    rolling_AVG_goals_for,
    rolling_AVG_goals_against,
    rolling_points
FROM final

    










