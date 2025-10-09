CREATE TABLE raw.raw_league_season(
	league_season_id SERIAL PRIMARY KEY NOT NULL,
	league_id INTEGER NOT NULL
		REFERENCES raw.raw_leagues(league_id),
	season_label VARCHAR(70) NOT NULL,
	start_date DATE NOT NULL,
	end_date DATE NOT NULL,
	is_current BOOLEAN NOT NULL DEFAULT FALSE,
	last_updated TIMESTAMPTZ DEFAULT NOW()	
)