CREATE TABLE raw_fixtures(
	fixture_id BIGINT PRIMARY KEY,
	date TIMESTAMP,
	season INTEGER,

	league_id INTEGER,
	league VARCHAR(100),
	country VARCHAR(100),

	-- Teams
	home_team_id INTEGER,
	home_team_name VARCHAR(255),
	away_team_id INTEGER,
	away_team_name VARCHAR(255),

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
	status  VARCHAR(5)
);