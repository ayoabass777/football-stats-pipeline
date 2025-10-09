CREATE TABLE dim.dim_fixtures(
	fixture_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	api_fixture_id BIGINT NOT NULL UNIQUE,
	league_season_id INT NOT NULL 
		REFERENCES dim.dim_league_seasons(league_season_id),
	fixture_status TEXT NOT NULL,
	kickoff_utc TIMESTAMPTZ NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	updated_at TIMESTAMPTZ NOT NULL  DEFAULT now(),
	is_played BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX ON dim.dim_fixtures (league_season_id);
CREATE INDEX ON dim.dim_fixtures (match_day);
CREATE INDEX ON dim.dim_fixtures (kickoff_utc); -- to query by date and update

--ALTER TABLE dim.dim_fixtures
--ADD COLUMN is_played BOOLEAN
  --GENERATED ALWAYS AS (fixture_status = 'FT') STORED;