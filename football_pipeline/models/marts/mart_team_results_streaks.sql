{{ config(materialized='table') }}

WITH base AS (
    SELECT *
    FROM {{ ref('int_team_match_results')}}
),

ranked AS (
    SELECT *,
        ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY date) AS match_number,
        CASE WHEN result = 'win' THEN 1 ELSE 0 END AS is_win,
        CASE WHEN result = 'draw' THEN 1 ELSE 0 END AS is_draw,
        CASE WHEN result = 'loss' THEN 1 ELSE 0 END AS is_loss,
        CASE WHEN result != 'win' THEN 1 ELSE 0 END AS is_loss_or_draw,
        CASE WHEN result != 'loss' THEN 1 ELSE 0 END AS is_win_or_draw
    FROM base
),

last_10_results AS (
    SELECT *,
        SUM(is_win) OVER (
            PARTITION BY team_id 
            ORDER BY match_number
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS win_in_last_10,

        SUM(is_draw) OVER (
            PARTITION BY team_id 
            ORDER BY match_number
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS draw_in_last_10,

        SUM(is_loss) OVER (
            PARTITION BY team_id 
            ORDER BY match_number
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS loss_in_last_10,

        SUM(is_loss_or_draw) OVER (
            PARTITION BY team_id
            ORDER BY match_number
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS loss_or_draw_in_last_10,

        SUM(is_win_or_draw) OVER (
            PARTITION BY team_id
            ORDER BY match_number
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS win_or_draw_in_last_10
    FROM ranked
),

streak_groups AS (
    SELECT *,
        SUM(CASE WHEN result != 'win' THEN 1 ELSE 0 END)
        OVER (PARTITION BY team_id ORDER BY match_number) AS win_streak_group,

        SUM(CASE WHEN result != 'draw' THEN 1 ELSE 0 END)
        OVER (PARTITION BY team_id ORDER BY match_number) AS draw_streak_group,

        SUM(CASE WHEN result != 'loss' THEN 1 ELSE 0 END)
        OVER (PARTITION BY team_id ORDER BY match_number) AS loss_streak_group,

        SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END)
        OVER(PARTITION BY team_id ORDER BY match_number) AS win_or_draw_streak_group,

        SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END)
        OVER (PARTITION BY team_id ORDER BY match_number) AS loss_or_draw_streak_group
    FROM last_10_results
),

final AS (
    SELECT *,

        -- General streaks
        CASE WHEN result ='win' 
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, win_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_win_streak,      
        
        CASE WHEN result = 'draw'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, draw_streak_group ORDER BY match_number
            )
            ELSE 0
        END AS current_draw_streak,

        CASE WHEN result = 'loss'
            THEN ROW_NUMBER() OVER (
                    PARTITION BY team_id, loss_streak_group ORDER BY match_number
                )
            ELSE 0
        END AS current_loss_streak,

        CASE WHEN result IN ('win', 'draw') 
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, win_or_draw_streak_group ORDER BY match_number
            )
            ELSE 0
        END AS current_win_or_draw_streak,
        
        CASE WHEN result IN ('loss', 'draw') 
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, loss_or_draw_streak_group ORDER BY match_number
            )
            ELSE 0
        END AS current_loss_or_draw_streak,

        --Home Streaks
        CASE WHEN result = 'win' AND venue = 'home'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, win_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_home_win_streak,

         CASE WHEN result ='draw' AND venue = 'home'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, draw_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_home_draw_streak, 

        CASE WHEN result ='loss' AND venue = 'home'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, loss_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_home_loss_streak,

        CASE WHEN result IN ('win', 'draw') AND venue = 'home'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, win_or_draw_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_home_win_or_draw_streak,

        CASE WHEN result IN ('loss', 'draw') AND venue = 'home'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, loss_or_draw_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_home_loss_or_draw_streak,

        -- Away streaks
        CASE WHEN result = 'win' AND venue = 'away'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, win_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_away_win_streak,

         CASE WHEN result ='draw' AND venue = 'away'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, draw_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_away_draw_streak, 

        CASE WHEN result ='loss' AND venue = 'away'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, loss_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_away_loss_streak,

        CASE WHEN result IN ('win', 'draw') AND venue = 'away'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, win_or_draw_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_away_win_or_draw_streak,

        CASE WHEN result IN ('loss', 'draw') AND venue = 'away'
            THEN ROW_NUMBER() OVER (
                PARTITION BY team_id, venue, loss_or_draw_streak_group ORDER BY match_number
                )
            ELSE 0 
        END AS current_away_loss_or_draw_streak

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
    result,
    match_number,

    --10-game summary
    win_in_last_10,
    draw_in_last_10,
    loss_in_last_10,
    win_or_draw_in_last_10,
    loss_or_draw_in_last_10,

    --general streaks
    current_win_streak,
    current_draw_streak,
    current_loss_streak,
    current_win_or_draw_streak,
    current_loss_or_draw_streak,
    
    --home streaks
    current_home_win_streak,
    current_home_draw_streak,
    current_home_loss_streak,
    current_home_win_or_draw_streak,
    current_home_loss_or_draw_streak,

    --away streaks
    current_away_win_streak,
    current_away_draw_streak,
    current_away_loss_streak,
    current_away_win_or_draw_streak,
    current_away_loss_or_draw_streak
    
FROM final