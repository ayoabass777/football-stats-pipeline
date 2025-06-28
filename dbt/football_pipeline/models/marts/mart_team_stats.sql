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
        COUNT(*) AS games_played,
        SUM(CASE WHEN result ='win' THEN 1 ELSE 0) AS wins,
        SUM(CASE WHEN result='loss' THEN 1 ELSE 0) AS losses,
        SUM(CASE WHEN result='draw' THEN 1 ELSE 0) AS draws,
        SUM(goals_scored) AS goals_for,
        SUM(goals_conceded) AS goals_against
    FROM base
    GROUP BY team_id, season, league_id
),

rates_and_average AS (
    SELECT *,
        (wins:: FLOAT / games_played) AS win_rate,
        (losses:: FLOAT / games_played) AS loss_rate,
        AVG(goals_scored) AS AVG_goals_for_per_game,
        AVG(goals_against) AS AVG_goals_against_per_game
    FROM games_played_stats
),

final AS(
    SELECT *
    FROM rates_and_average
)

SELECT
    team_name,
    league,
    country,
    season,
    games_played,
    wins,
    losses,
    draws,
    goals_for,
    goals_against,
    win_rate,
    loss_rate,
    AVG_goals_for_per_game,
    AVG_goals_against_per_game
FROM final;

    










