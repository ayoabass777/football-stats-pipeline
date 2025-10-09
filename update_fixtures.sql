SELECT *
FROM raw.raw_fixtures AS rf
WHERE rf.kickoff_utc < NOW()
              AND (rf.home_team_fulltime_goal IS NULL OR rf.away_team_fulltime_goal IS NULL) AND rf.season = 2025
ORDER BY kickoff_utc ASC
