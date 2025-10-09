SELECT
                ls.league_season_id,
                l.api_league_id,
				l.league_name,
                ls.season,
                rf.kickoff_utc,
				rf.api_fixture_id
            FROM raw.raw_fixtures rf
            JOIN raw_stg.stg_dim_leagues l
                    ON rf.api_league_id = l.api_league_id
			JOIN raw_stg.stg_dim_league_seasons ls
              ON l.league_id = ls.league_id
              AND ls.is_current = TRUE
            WHERE
              rf.kickoff_utc < NOW()
              AND (rf.home_team_fulltime_goal IS NULL OR rf.away_team_fulltime_goal IS NULL)
