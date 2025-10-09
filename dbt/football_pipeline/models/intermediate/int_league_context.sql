{{config (materialized='table', schema='int')}}

WITH base as (
    SELECT
        country_name,
        league_name,
        l.league_id,
        api_league_id,
        season,
        ls.league_season_id,
        is_current

    FROM {{ ref('stg_dim_league_seasons') }} ls
    JOIN {{ ref('stg_dim_leagues') }} l
      ON ls.league_id = l.league_id
    JOIN {{ ref('stg_dim_countries') }} c
      ON l.country_id = c.country_id
)

SELECT *
FROM base
