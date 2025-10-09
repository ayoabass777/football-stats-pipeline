{{ config (materialized='table', schema='int')}}

SELECT 
    team_id,
    api_team_id,
    initcap(team_name) as team_name
FROM {{ ref('stg_dim_teams') }}
