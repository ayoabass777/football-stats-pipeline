{{ config(materialized='table') }}

With base AS (
    SELECT *
    FROM {{ ref('int_team_result_streaks')}}
),

latest_streak AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY team_id, event_name ORDER BY date DESC) AS rn
        FROM base
),

final AS (
    SELECT
        fixture_id,
        date,
        team_name,
        team_id,
        result,
        event_name,
        streak_count
    FROM latest_streak
    WHERE rn = 1 AND streak_count > 2
)

SELECT 
    date,
    fixture_id,
    team_name,
    event_name,
    streak_count
FROM final
ORDER BY event_name, streak_count DESC


