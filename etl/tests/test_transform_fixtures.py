

import pandas as pd
import pytest
from etl.src.transform_fixtures import (
    handle_special_results,
    compute_game_results,
    compute_results
)

def make_df(status, h_ft, a_ft, h_ht=None, a_ht=None):
    """Helper to create a minimal DataFrame for testing."""
    data = {
        "fixture_status": [status],
        "home_team_fulltime_goal": [h_ft],
        "away_team_fulltime_goal": [a_ft],
        "home_team_halftime_goal": [h_ht if h_ht is not None else pd.NA],
        "away_team_halftime_goal": [a_ht if a_ht is not None else pd.NA],
    }
    return pd.DataFrame(data)


@pytest.mark.parametrize("status, h_ft, a_ft, expected_home, expected_away", [
    ("WO", 3, 0, "win", "loss"),
    ("WO", 0, 3, "loss", "win"),
    ("WO", 1, 1, "draw", "draw"),
    ("AWD", 2, 2, "draw", "draw"),
])
def test_handle_special_results(status, h_ft, a_ft, expected_home, expected_away):
    # Halftime values not tested here
    df = make_df(status, h_ft, a_ft)
    df2 = handle_special_results(df.copy())
    assert df2.at[0, "home_fulltime_result"] == expected_home
    assert df2.at[0, "away_fulltime_result"] == expected_away

def test_compute_game_results_normal():
    df = make_df("FT", 2, 1, 1, 1)
    df2 = compute_game_results(df.copy())
    assert df2.at[0, "home_fulltime_result"] == "win"
    assert df2.at[0, "away_fulltime_result"] == "loss"
    assert df2.at[0, "home_halftime_result"] == "draw"
    assert df2.at[0, "away_halftime_result"] == "draw"

def test_compute_results_integration():
    # Mixed statuses
    df = pd.concat([
        make_df("FT", 2, 0, 1, 0),
        make_df("WO", 3, 0),
        make_df("AB", 1, 1, 0, 0),
        make_df("FT", pd.NA, pd.NA),
    ], ignore_index=True)
    df2 = compute_results(df.copy())
    # Row0: FT normal
    assert df2.at[0, "home_fulltime_result"] == "win"
    # Row1: WO
    assert df2.at[1, "home_fulltime_result"] == "win"
    # Row2: AB
    assert df2.at[2, "home_fulltime_result"] is None
    # Row3: no scores
    assert df2.at[3, "home_fulltime_result"] is pd.NA or df2.at[3, "home_fulltime_result"] is None

    # Row1: WO should have no halftime results
    assert pd.isna(df2.at[1, "home_halftime_result"])
    assert pd.isna(df2.at[1, "away_halftime_result"])

    # Row2: AB should have all results None
    for col in [
        "home_fulltime_result", "away_fulltime_result",
        "home_halftime_result", "away_halftime_result"
    ]:
        assert df2.at[2, col] is None

    # Row3: FT with no scores should have NA results
    for col in [
        "home_fulltime_result", "away_fulltime_result",
        "home_halftime_result", "away_halftime_result"
    ]:
        assert pd.isna(df2.at[3, col])