{{ config(materialized='table') }}

with ranked as (
  select
    team_id,
    team_name,
    league_season_id,
    league_name,
    event_name,
    streak_type,
    event_flag,
    streak_length,
    row_number() over (
      partition by team_id, event_name
      order by kickoff_utc desc
    ) as rn
  from {{ ref('fact_team_streaks') }}
),
current_streaks as (
  select
    team_id,
    team_name,
    league_season_id,
    league_name,
    event_name,
    streak_type,
    case when event_flag = 1 then streak_length else 0 end as current_streak_length
  from ranked
  where rn = 1 
)

select
  row_number() over (order by team_id, league_season_id, event_name, streak_type) as streak_id,
  league_name,
  team_name,
  event_name,
  streak_type,
  current_streak_length
from current_streaks
where current_streak_length > 2
order by
  current_streak_length desc,
  team_name
