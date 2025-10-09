CREATE TABLE dim.dim_teams (
  team_id       BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  api_team_id   INT          NOT NULL UNIQUE,
  team_name     TEXT         NOT NULL,
  team_code     VARCHAR(5),
  country_id    INT          NOT NULL REFERENCES dim.dim_countries(country_id),
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);