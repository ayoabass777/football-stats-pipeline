

{{ config(materialized='view') }}

--------------------------------------------------------------------------------
-- Intermediate: fact_team_streaks
-- Compute running streaks for each team and event using run-length encoding
--------------------------------------------------------------------------------

with numbered as (
    select
        team_id,
        team_name,
        league_season_id,
        league_name,
        event_name,
        fixture_id,
        kickoff_utc,
        event_flag,
        streak_type,
        row_number() over (
            partition by team_id, event_name
            order by kickoff_utc
        ) as rn,
        lag(event_flag, 1, 0) over (
            partition by team_id, event_name
            order by kickoff_utc
        ) as prev_flag
    from {{ ref('fact_team_events') }}
    where is_current and is_played
),

grouped as (
    select
        team_id,
        team_name,
        league_season_id,
        league_name,
        event_name,
        streak_type,
        fixture_id,
        kickoff_utc,
        event_flag,
        rn,
        sum(
            case when event_flag = 1 and prev_flag = 0 then 1 else 0 end
        ) over (
            partition by team_id, event_name
            order by rn
        ) as streak_id
    from numbered
),

streaks as (
    select
        team_id,
        team_name,
        league_season_id,
        league_name,
        event_name,
        streak_type,
        fixture_id,
        kickoff_utc,
        event_flag,
        case
            when event_flag = 1 then 
                row_number() over (
                    partition by team_id, event_name, streak_id
                    order by rn
                )
            else 0
        end as streak_length
    from grouped
)

select * from streaks