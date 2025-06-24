/*
 * mart/intermediate model: int_team_goal_streaks
 * This model computes goal and concession streaks for each team by event type.
 * Steps:
 * 1. Load raw match goals from int_team_match_goals.
 * 2. Unpivot to create one row per event flag (goal scored, concession, etc.).
 * 3. Compute lag to detect where streaks start.
 * 4. Assign a streak group ID to consecutive runs of the same event.
 * 5. Count the length of each streak.
 * 6. Output all rows so zeros are preserved when streaks break.
 */
{{ config(materialized='table') }}

-- CTE: base
-- Load all columns from the intermediate model containing team match goals.
WITH base AS(
    SELECT *
        FROM {{ ref('int_team_match_goals')}}
),

-- CTE: event_indicators
-- Unpivot goals_scored and goals_conceded into separate event rows.
-- For each team and fixture, create one row per event type with a boolean flag.
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

-- CTE: lag_event_indicator
-- Calculate the previous row's event_flag for each team and event type,
-- defaulting to 0 when no prior row exists.
lag_event_indicator AS(
    SELECT *,
        LAG(ev_flag, 1, 0)
            OVER (PARTITION BY team_id, event_name ORDER BY date) AS lag_ev_flag
    FROM event_indicators
),

-- CTE: streak_grouped
-- Assign a unique group ID to each new streak.
-- A streak starts when ev_flag = 1 and the previous lag_ev_flag = 0.
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

-- CTE: streaks
-- For each streak group, count consecutive occurrences of the event.
-- Rows where ev_flag = 0 get a streak_count of 0 to preserve breaks.
streaks AS (    
    SELECT *,
        -- count rows within each (team_id, event_name, streak_grp) for which ev_flag =1 (True)
        -- using case when for when the ev_flag is true or streak count should be zero
        CASE WHEN ev_flag = 1 
            THEN ROW_NUMBER() OVER(PARTITION BY team_id, event_name, streak_grp ORDER BY date)
        ELSE 0 END AS streak_count
    FROM streak_grouped
),

-- CTE: final
-- Select all relevant columns, preserving zero rows so downstream models
-- can detect when a streak has ended.
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

)


-- Final select: emit every row (including zeros) so later CTEs can pick
-- the last row per team/event to show current streak status.
SELECT * FROM final




