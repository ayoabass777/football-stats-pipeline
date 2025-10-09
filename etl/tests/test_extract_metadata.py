import sys
import os
import yaml
import pytest
from etl.src.extract_metadata import load_yaml, upsert_country, upsert_league, upsert_league_season

class DummyCursor:
    def __init__(self, fetchone_responses):
        self.fetchone_responses = list(fetchone_responses)
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((sql.strip(), params))

    def fetchone(self):
        return self.fetchone_responses.pop(0)

def test_load_yaml(tmp_path):
    data = {"foo": "bar"}
    yaml_file = tmp_path / "test.yaml"
    with open(yaml_file, "w") as f:
        yaml.dump(data, f)
    assert load_yaml(str(yaml_file)) == data

def test_upsert_country_insert():
    cur = DummyCursor(fetchone_responses=[(1,)])
    assert upsert_country(cur, "Spain") == 1
    assert any("INSERT INTO dim.dim_countries" in q[0] for q in cur.queries)

def test_upsert_country_select():
    cur = DummyCursor(fetchone_responses=[None, (2,)])
    assert upsert_country(cur, "Italy") == 2
    assert cur.queries[1][0].startswith("SELECT country_id FROM dim.dim_countries")

def test_upsert_league_insert():
    cur = DummyCursor(fetchone_responses=[(3,)])
    league_id = upsert_league(cur, 10, "TestLeague", 1234)
    assert league_id == 3
    sql, params = cur.queries[0]
    assert "INSERT INTO dim.dim_league" in sql
    assert params == (10, "TestLeague", 1234)

def test_upsert_league_season_insert():
    cur = DummyCursor(fetchone_responses=[(4,)])
    sid = upsert_league_season(cur, 10, 2021, "2021/22", "2021-08-01", "2022-05-22")
    assert sid == 4
    sql, params = cur.queries[0]
    assert "INSERT INTO dim.dim_league_seasons" in sql
    assert params == (10, 2021, "2021/22", "2021-08-01", "2022-05-22")