{{ config(
    materialized='view'
) }}

--------------------------------------------------------------------------------
-- Intermediate: fact_team_events
-- Explodes each team_fixture_stats row into event flags per fixture
--------------------------------------------------------------------------------

WITH base AS (
    SELECT
        league_season_id,
        league_name,
        season,
        fixture_id,
        team_id,
        team_name,
        opponent_id,
        opponent_name,
        kickoff_utc,
        goals_for,
        goals_against,
        halftime_goals_for,
        halftime_goals_against,
        fulltime_result,
        halftime_result,
        is_home,
        is_current,
        is_played
    FROM {{ ref('fact_team_fixtures') }}
    WHERE is_played
    {% if is_incremental() %}
      AND updated_at > (SELECT max(updated_at) FROM {{ this }})
    {% endif %}
),

event_ind AS (
    SELECT
        b.league_season_id,
        b.league_name,
        b.season,
        b.fixture_id,
        b.team_id,
        b.team_name,
        b.opponent_id,
        b.opponent_name,
        b.kickoff_utc,
        b.goals_for,
        b.goals_against,
        b.fulltime_result,
        ev.event_name,
        ev.event_flag,
        ev.streak_type,
        b.is_home,
        b.is_current,
        b.is_played
    FROM base b
    CROSS JOIN lateral (
        SELECT *
        FROM (VALUES
            ('score_1goal',        (b.goals_for >= 1)::int,   'goal_streaks'),
            ('score_2goals',       (b.goals_for >= 2)::int,   'goal_streaks'),
            ('score_3goals',       (b.goals_for >= 3)::int,   'goal_streaks'),
            ('concede_1',          (b.goals_against >= 1)::int,   'goal_streaks'),
            ('concede_2',          (b.goals_against >= 2)::int,   'goal_streaks'),
            ('concede_3',          (b.goals_against >= 3)::int,   'goal_streaks'),
            ('goalless',           (b.goals_for = 0)::int,        'special_streaks'),
            ('clean_sheet',        (b.goals_against = 0)::int,     'special_streaks'),
            ('win',                (b.fulltime_result = 'win')::int,           'result_streaks'),
            ('draw',               (b.fulltime_result = 'draw')::int,          'result_streaks'),
            ('loss',               (b.fulltime_result = 'loss')::int,          'result_streaks'),
            ('win_or_draw',        (b.fulltime_result in ('win','draw'))::int,  'result_streaks'),
            ('halftime_win',       (b.halftime_result = 'win')::int,            'special_streaks'),
            ('win_draw_over_1_5',  ((b.fulltime_result IN ('win','draw')) AND (b.goals_for + b.goals_against) > 1.5)::int,  'special_streaks'),
            ('win_draw_under_4_5', ((b.fulltime_result IN ('win','draw')) AND (b.goals_for + b.goals_against) < 4.5)::int,  'special_streaks')
        ) AS ev(event_name, event_flag, streak_type)
    ) ev
)

SELECT * FROM event_ind
