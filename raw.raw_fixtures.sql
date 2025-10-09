CREATE TABLE raw.raw_fixtures (
  fixture_id                    SERIAL       PRIMARY KEY,
  api_fixture_id                BIGINT       NOT NULL,            -- external fixture ID
  api_league_id                 INT          NOT NULL,        -- external league ID
  season                        INT          NOT NULL,        -- API season key (e.g. 2025)
  kickoff_utc                   TIMESTAMPTZ  NOT NULL,
  fixture_status                TEXT         NOT NULL,
  home_team_id                  INT,
  home_team_name                TEXT,
  away_team_id                  INT,
  away_team_name                TEXT,
  home_team_halftime_goal       INT,
  away_team_halftime_goal       INT,
  home_team_fulltime_goal       INT,
  away_team_fulltime_goal       INT,
  home_fulltime_result          TEXT,
  away_fulltime_result          TEXT,
  home_halftime_result          TEXT,
  away_halftime_result          TEXT,
  created_at                    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at                    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX ON raw.raw_fixtures (fixture_id);
CREATE INDEX ON raw.raw_fixtures (api_league_id);
CREATE INDEX ON raw.raw_fixtures (api_fixture_id);
CREATE INDEX ON raw.raw_fixtures (kickoff_utc);