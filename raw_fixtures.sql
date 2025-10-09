CREATE TABLE raw_fixtures(
	fixture_id BIGINT PRIMARY KEY NOT NULL,
	match_date TIMESTAMP,
	season INTEGER NOT NULL,

	league_id INTEGER,
	league VARCHAR(100) NOT NULL,
	country VARCHAR(100) NOT NULL,

	-- Teams
	home_team_id INTEGER,
	home_team_name VARCHAR(255) NOT NULL,
	away_team_id INTEGER,
	away_team_name VARCHAR(255) NOT NULL,

	--Scores
	home_team_halftime_goal INTEGER,
	away_team_halftime_goal INTEGER,
	home_team_fulltime_goal INTEGER,
	away_team_fulltime_goal INTEGER,

	--Results (outcome)
	home_fulltime_result VARCHAR(10),
	away_fulltime_result VARCHAR(10),
	home_halftime_result VARCHAR(10),
	away_halftime_result VARCHAR(10),

	-- Match status: FT, NS, PST, etc.
	match_status  VARCHAR(5) NOT NULL
);