{{ config(materialized='table') }}

WITH base AS(
    SELECT *
        FROM {{ ref('int_team_match_goals')}}
),

streak_groups AS(
    SELECT *,

        -- Indicators
        -- "over_0_5" means goals_scored > 0, i.e., scored at least 1 goal
        -- "over_1_5" means goals_scored > 1, i.e., scored at least 2 goals
        CASE WHEN goals_scored > 0 THEN 1 ELSE 0 END AS team_scored_over_0_5,
        CASE WHEN goals_scored > 1 THEN 1 ELSE 0 END AS team_scored_over_1_5,
        CASE WHEN goals_conceded > 0 THEN 1 ELSE 0 END AS team_conceded_over_0_5,
        CASE WHEN goals_conceded > 1 THEN 1 ELSE 0 END AS team_conceded_over_1_5,
        CASE WHEN goals_scored = 0 THEN 1 ELSE 0 END AS team_goalless,
        CASE WHEN goals_conceded = 0 THEN 1 ELSE 0 END AS team_cleansheet,

        -- Grouping for streak windows
        SUM(CASE WHEN goals_scored < 1 THEN 1 ELSE 0 END ) OVER (
            PARTITION BY team_id ORDER BY date
        ) AS team_scored_over_0_5_streak_group,

        SUM(CASE WHEN goals_scored < 2 THEN 1 ELSE 0 END ) OVER (
            PARTITION BY team_id ORDER BY date
        ) AS team_scored_over_1_5_streak_group,

        SUM(CASE WHEN goals_conceded < 1 THEN 1 ELSE 0 END ) OVER (
            PARTITION BY team_id ORDER BY date
        ) AS team_conceded_over_0_5_streak_group,

        SUM(CASE WHEN goals_conceded < 2 THEN 1 ELSE 0 END ) OVER (
            PARTITION BY team_id ORDER BY date
        ) AS team_conceded_over_1_5_streak_group,

        SUM(CASE WHEN goals_scored = 0 THEN 1 ELSE 0 END ) OVER (
            PARTITION BY team_id ORDER BY date
        ) as team_goalless_streak_group,

        SUM(CASE WHEN goals_conceded = 0 THEN 1 ELSE 0 END ) OVER (
            PARTITION BY team_id ORDER BY date
        ) as team_cleansheet_streak_group
    FROM base
),

final AS(
    SELECT *,

    -- Scoring Streaks
    CASE WHEN goals_scored > 0 THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_scored_over_0_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS team_scored_over_0_5_streak,

    CASE WHEN goals_scored > 1 THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_scored_over_1_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS team_scored_over_1_5_streak,

    -- Conceding Streaks
    CASE WHEN goals_conceded > 0 THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_conceded_over_0_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS team_conceded_over_0_5_streak,

    CASE WHEN goals_conceded > 1 THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_conceded_over_1_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS team_conceded_over_1_5_streak,

    -- Defensive Streaks
    CASE WHEN goals_conceded = 0 THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_cleansheet_streak_group
        ORDER BY date)
        ELSE 0 
    END AS team_cleansheet_streak,

    CASE WHEN goals_scored = 0 THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_goalless_streak_group
        ORDER BY date)
        ELSE 0 
    END AS team_goalless_streak,

    -- Home Scoring Streaks
    CASE WHEN goals_scored > 0 AND venue = 'home' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_scored_over_0_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS home_team_scored_over_0_5_streak,

    CASE WHEN goals_scored > 1 AND venue = 'home' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_scored_over_1_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS home_team_scored_over_1_5_streak,

    -- Home Conceding Streaks
    CASE WHEN goals_conceded > 0 AND venue = 'home' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_conceded_over_0_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS home_team_conceded_over_0_5_streak,

    CASE WHEN goals_conceded > 1 AND venue = 'home' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_conceded_over_1_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS home_team_conceded_over_1_5_streak,

    -- Home Defensive Streaks
    CASE WHEN goals_conceded = 0 AND venue = 'home' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_cleansheet_streak_group
        ORDER BY date)
        ELSE 0 
    END AS home_team_cleansheet_streak,

    CASE WHEN goals_scored = 0 AND venue = 'home' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_goalless_streak_group
        ORDER BY date)
        ELSE 0 
    END AS home_team_goalless_streak,

    -- Away Scoring Streaks
    CASE WHEN goals_scored > 0 AND venue = 'away' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_scored_over_0_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS away_team_scored_over_0_5_streak,

    CASE WHEN goals_scored > 1 AND venue = 'away' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_scored_over_1_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS away_team_scored_over_1_5_streak,

    -- Away Conceding Streaks
    CASE WHEN goals_conceded > 0 AND venue = 'away' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_conceded_over_0_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS away_team_conceded_over_0_5_streak,

    CASE WHEN goals_conceded > 1 AND venue = 'away' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_conceded_over_1_5_streak_group
        ORDER BY date)
        ELSE 0 
    END AS away_team_conceded_over_1_5_streak,

    -- Away Defensive Streaks
    CASE WHEN goals_conceded = 0 AND venue = 'away' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_cleansheet_streak_group
        ORDER BY date)
        ELSE 0 
    END AS away_team_cleansheet_streak,

    CASE WHEN goals_scored = 0 AND venue = 'away' THEN 
        ROW_NUMBER() OVER (PARTITION BY team_id, team_goalless_streak_group
        ORDER BY date)
        ELSE 0 
    END AS away_team_goalless_streak
    
FROM streak_groups
)

SELECT
    --General info
    fixture_id,
    date,
    season,
    team_id,
    opponent_id,
    league,
    league_id,
    country,
    venue,

    --GOAL SCORED
    team_scored_over_0_5_streak,
    team_scored_over_1_5_streak,

    --GOAL CONCEDED
    team_conceded_over_0_5_streak,
    team_conceded_over_1_5_streak,

    --CLEANSHEET
    team_cleansheet_streak,

    -- TEAM GOALLESS STREAK
    team_goalless_streak,

    -- HOME GOAL SCORED
    home_team_scored_over_0_5_streak,
    home_team_scored_over_1_5_streak,

    -- HOME GOAL CONCEDED
    home_team_conceded_over_0_5_streak,
    home_team_conceded_over_1_5_streak,

    -- HOME CLEANSHEET
    home_team_cleansheet_streak,

    -- HOME TEAM GOALLESS STREAK
    home_team_goalless_streak,

    -- AWAY GOAL SCORED
    away_team_scored_over_0_5_streak,
    away_team_scored_over_1_5_streak,

    -- AWAY GOAL CONCEDED
    away_team_conceded_over_0_5_streak,
    away_team_conceded_over_1_5_streak,

    -- AWAY CLEANSHEET
    away_team_cleansheet_streak,

    -- AWAY TEAM GOALLESS STREAK
    away_team_goalless_streak
FROM final
