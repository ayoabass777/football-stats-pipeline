{{ config(materialized='table') }}

WITH base AS (
    SELECT * 
        FROM {{ ref('mart_team_results_streaks')}}
),

latest_per_team AS (
    SELECT *
    FROM(
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY date DESC) AS rn
        FROM base
    )
    WHERE rn = 1
)

SELECT
    fixture_id,
    date,
    season,
    team_id,
    opponent_id,
    venue,
    league,
    league_id,
    country,
    result,

    -- Last 10 performance
    win_in_last_10,
    draw_in_last_10,
    loss_in_last_10,
    win_or_draw_in_last_10,
    loss_or_draw_in_last_10,

    -- General streaks
    current_win_streak,
    current_draw_streak,
    current_loss_streak,
    current_win_or_draw_streak,
    current_loss_or_draw_streak,

    -- Home
    current_home_win_streak,
    current_home_draw_streak,
    current_home_loss_streak,
    current_home_win_or_draw_streak,
    current_home_loss_or_draw_streak,

    -- Away
    current_away_win_streak,
    current_away_draw_streak,
    current_away_loss_streak,
    current_away_win_or_draw_streak,
    current_away_loss_or_draw_streak

FROM latest_per_team