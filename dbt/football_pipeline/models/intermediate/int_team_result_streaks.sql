{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('int_team_match_results')}}
),

-- Unpivot all indicators into (team_id, date, fixtures,..., event_name, event_flag)
event_indicator AS (
    SELECT 
        b.fixture_id,
        b.date,
        b.team_id,
        b.team_name,
        b.result,
        ev.event_name AS event_name,
        --convert ev_flag to int from boolean
        ev.event_flag:: int AS event_flag
    FROM base AS b
    CROSS JOIN LATERAL (
        VALUES
            ('win', b.result = 'win'),
            ('draw', b.result = 'draw'),
            ('loss', b.result = 'loss'),

            ('win_or_draw', b.result in ('win','draw')),
            ('loss_or_draw', b.result in ('loss','draw'))
    ) AS ev(event_name, event_flag) -- ev means event
),

-- Compute the lag of each event per team and event name
lag_event_indicator AS (
    SELECT *,
        LAG(event_flag,1,0) OVER(PARTITION BY team_id, event_name ORDER BY date) AS lag_ev_flag
    FROM event_indicator
),

-- Build a running group ID for each new streak
streak_grouped AS (
    SELECT *,
        -- start a new group whenever ev_flag = 1 and prior was 0
        SUM(
            CASE 
                WHEN event_flag = 1 AND lag_ev_flag = 0 THEN 1 
                ELSE 0 
            END
            ) OVER (PARTITION BY team_id, event_name ORDER BY date) AS streak_grp
    FROM lag_event_indicator
),
-- CTE: streaks
-- For each streak group, count consecutive occurrences of the event.
-- Rows where ev_flag = 0 get a streak_count of 0 to preserve breaks.
streaks AS (
    SELECT *,
        -- count rows within each (team_id, event_name, streak_grp) for which ev_flag =1 (True)
        -- using case when for when the ev_flag is true or streak count should be zero
        CASE WHEN event_flag = 1 
            THEN ROW_NUMBER() OVER (PARTITION BY team_id, event_name, streak_grp ORDER BY date) 
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
        result,
        event_name,
        event_flag,
        lag_ev_flag,
        streak_grp,
        streak_count
    FROM streaks
)

-- Final select: emit every row (including zeros) so later CTEs can pick
-- the last row per team/event to show current streak status.
SELECT * FROM final