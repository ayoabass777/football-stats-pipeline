{{ config(materialized='table') }}

WITH base AS(
    SELECT *
        FROM {{ ref('int_team_match_goals')}}
),

-- Unpivot all indicators into (team_id, date, fixtures,..., event_name, event_flag)
event_indicators AS (
    SELECT 
        b.fixture_id,
        b.date,
        b.team_id,
        b.team_name,
        b.goals_scored,
        b.goals_conceded,
        ev.event_name,
        --convert ev_flag to int from boolean
        ev.ev_flag::int 
    FROM base as b
    CROSS JOIN LATERAL (
        VALUES
            ('score_1goal', b.goals_scored >= 1),
            ('score_2goal', b.goals_scored >= 2),

            ('concede_1goal', b.goals_conceded > 0),
            ('concede_2goal', b.goals_conceded > 1),

            ('goalless', b.goals_scored = 0),
            ('clean_sheet', b.goals_conceded = 0)
    ) AS ev(event_name, ev_flag) -- ev means event
),

-- Compute the lag of each event per team and event name
lag_event_indicator AS(
    SELECT *,
        LAG(ev_flag, 1, 0)
            OVER (PARTITION BY team_id, event_name ORDER BY date) AS lag_ev_flag
    FROM event_indicators
),

-- Build a running group ID for each new streak
streak_grouped AS (  
    SELECT *,
        -- start a new group whenever ev_flag = 1 and prior was 0
        SUM(
            CASE
                WHEN ev_flag = 1 AND lag_ev_flag = 0 THEN 1
                ELSE 0
            END
            ) OVER(PARTITION BY team_id, event_name ORDER BY date) AS streak_grp

    FROM lag_event_indicator
),

streaks AS (
    
    SELECT *,
        -- count rows within each (team_id, event_name, streak_grp) for which ev_flag =1 (True)
        ROW_NUMBER() OVER(PARTITION BY team_id, event_name, streak_grp ORDER BY date)
          AS streak_count
    FROM streak_grouped
    WHERE ev_flag = 1
),

final AS (
    SELECT 
        fixture_id,
        date,
        team_id,
        team_name,
        goals_scored,
        goals_conceded,
        event_name,
        ev_flag,
        lag_ev_flag,
        streak_grp,
        streak_count
    FROM streaks
    ORDER BY event_name, date

)
SELECT * FROM final




