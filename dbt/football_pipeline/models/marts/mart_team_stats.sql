{{ config(materialized='table')}}

WITH base AS(
    SELECT
        r.fixture_id AS fixture_id,
        r.date AS date,
        r.season AS season,
        r.league_id AS league_id,
        r.country AS country,
        r.league AS league,
        r.team_id AS team_id,
        r.team_name AS team_name,
        r.opponent_id AS opponent_id,
        r.opponent_name AS opponent_name,
        r.venue AS venue,
        r.result AS result,
        g.goals_scored AS goals_scored,
        g.goals_conceded AS goals_conceded
    FROM {{ ref('int_team_match_results')}} AS r
    LEFT JOIN {{ ref('int_team_match_goals')}} AS g
    ON r.fixture_id = g.fixture_id AND r.team_id = g.team_id
),

games_played_stats AS (
    SELECT *,
        COUNT(*) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS games_played,
        SUM(CASE WHEN result ='win' THEN 1 ELSE 0 END) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS wins,
        SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS losses,
        SUM(CASE WHEN result='draw' THEN 1 ELSE 0 END) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS draws,
        SUM(goals_scored) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS goals_for,
        SUM(goals_conceded) OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS goals_against,
        ROW_NUMBER() OVER(PARTITION BY league_id, season, team_id ORDER BY date) AS matchday
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

last_5_form AS (
    SELECT *,
        array_agg(CASE WHEN result = 'win' THEN 'W'
                        WHEN result = 'draw' THEN 'D'
                        WHEN result = 'loss' THEN 'L'
                        ELSE NULL END)
            OVER(PARTITION BY league_id, season, team_id 
                ORDER BY matchday
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW ) AS form 
    FROM rolling_pts
),

final AS(
    SELECT *
    FROM last_5_form
)

SELECT
    date,
    matchday,
    team_name,
    league,
    country,
    season,
    games_played,
    form,
    wins,
    losses,
    draws,
    goals_for,
    goals_against,
    rolling_win_rate,
    rolling_draw_rate,
    rolling_loss_rate,
    rolling_AVG_goals_for,
    rolling_AVG_goals_against,
    rolling_points
FROM final

    










