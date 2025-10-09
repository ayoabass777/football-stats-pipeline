SELECT DISTINCT srf.fixture_id
FROM raw_stg.stg_raw_fixtures as srf
JOIN raw_stg.stg_dim_leagues l
ON srf.api_league_id = l.api_league_id
JOIN raw_stg.stg_dim_league_seasons ls
ON l.league_id = ls.league_id
	AND ls.season = srf.season
WHERE Srf.kickoff_utc < NOW() - INTERVAL '2 hour'
      AND (srf.home_team_fulltime_goal IS NULL OR srf.away_team_fulltime_goal IS NULL)
	  AND ls.is_current

