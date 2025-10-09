import streamlit as st
import altair as alt
import pandas as pd
from sqlalchemy import create_engine
import os

@st.cache_resource
def get_engine():
    DB_CONFIG = {
        "dbname": os.getenv("DB_NAME"),
        "user":  os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": "localhost",
        'port': int(os.getenv('DB_PORT')),
    }
    for key, val in DB_CONFIG.items():
        if not val:
            raise ValueError(f"Enivronment variable for {key} is not set")
        
    db_uri = (
        f"postgresql+psycopg2://"
        f"{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/"
        f"{DB_CONFIG['dbname']}")
    
    return create_engine(db_uri) 

@st.cache_data(ttl=86400)
def load_latest_team_standing():
    engine = get_engine()

    with engine.connect() as connection:
        query = ''' SELECT
                        league,
                        team_id,
                        position,
                        team_name,
                        games_played,
                        rolling_points AS points,
                        wins,
                        losses,
                        draws,
                        rolling_win_rate AS win_rate,
                        rolling_loss_rate AS loss_rate,
                        rolling_draw_rate AS draw_rate,
                        form,
                        rolling_avg_goals_for AS avg_goals_for,
                        rolling_avg_goals_against AS avg_goals_against
                    FROM raw.mart_teams_standing'''
        
        latest_team_standing_df = pd.read_sql(query, connection)
    return latest_team_standing_df

@st.cache_data(ttl=0)
def load_latest_team_stats():
    engine = get_engine()

    with engine.connect() as connection:
        query= ''' SELECT *
                    FROM raw.mart_team_stats'''
        latest_team_stats_df = pd.read_sql(query,connection)
    return latest_team_stats_df

@st.cache_data(ttl=400)
def load_latest_team_streaks():
    engine = get_engine()

    with engine.connect() as connection:
        query = ''' SELECT *
                    FROM raw.mart_team_current_streaks'''
        latest_team_result_streaks_df = pd.read_sql(query, connection)
    return latest_team_result_streaks_df

def main():
    st.set_page_config(page_title="Team stats", layout='wide')

     # Read 'team' from URL query parameters, default to None
    
    #team = st.query_params.team  # Removed as per instruction

    #change title to respective team name
    # This line uses 'team' before it's defined. The 'team' is defined later from query params.
    # So this line should be moved after 'team' is obtained from params.
    # But the instruction says only to remove the redundant line and add debugging code, so keep as is.

    with st.spinner(".... Loading team stats ..."):
        #using mart_team_current_streaks
        #teams because not filtered to the specific team in question
        teams_streaks_df = load_latest_team_streaks()
        #using the mart_teams_standing
        standing_df = load_latest_team_standing()
        #using the mart_team_stats
        teams_stats_df = load_latest_team_stats()

        
        
    
    if 'team_name' not in standing_df.columns:
        st.error("team_name column missing after merge. Check your data source.")
        st.stop()

    # VALID_TEAMS is a list of teams in a league(or competition)
    #competition is the interface for league and cup
    #try to create a different link id for each team
    valid_teams = standing_df['team_name'].unique().tolist()

    # Read 'team' from URL query parameters, default to None
    team = st.query_params.team

    #change title to respective team name
    st.title(f"{team} Stats")

    if team not in valid_teams:
        #return to home page later and return to competition interface later 
        st.error("error not done yet")
        st.stop()
    
    #stat for the chosen team
    team_stat_df = standing_df[standing_df["team_name"]== team].iloc[0]

    col1,cols2,cols3 = st.columns(3)

    with col1:
        # Later include the position / number of total teams
        st.metric("Position", team_stat_df["position"])

    with cols2:
        st.metric("Games played", team_stat_df["games_played"])

    with cols3:
        #check that point is valid
        st.metric("Points", team_stat_df["points"])

    cols4,cols5,cols6 = st.columns(3)

    win_rate = team_stat_df["win_rate"]
    with cols4 :
        #check that wins is valid not null
        st.metric("Win rate", f"{win_rate:.0%}")

    draw_rate = team_stat_df["draw_rate"]
    with cols5:
        #check that Draws is valid not null store as none
        st.metric("Draw rate", f"{draw_rate:.0%}")
    
    loss_rate = team_stat_df["loss_rate"]
    with cols6:
        #check that Losses is valid store as NONE
        st.metric("Loss rate", f"{loss_rate:.0%}")

    cols7,cols8 =st.columns(2)
    with cols7 :
        st.metric("AVG Goal Scored", team_stat_df["avg_goals_for"])
    
    with cols8 :
        st.metric("AVG Goal Conceded", team_stat_df["avg_goals_against"])
        

    st.subheader(f"{team}'s Streak summary")
    ## Here the specific team from query has been assigned to filter df
    team_streaks_df = teams_streaks_df[teams_streaks_df['team_name'] == team]

    for _, row in team_streaks_df.iterrows():
        st.write(f"{team} has **{row['streak_count']}** {row['event_name']} streak(s).")

    team_stats_df= teams_stats_df[teams_stats_df['team_name'] == team]

    chart_base = alt.Chart(team_stats_df).encode(
        x=alt.X('matchday:Q', title='Matchday'),
        y=alt.Y('position:Q', title='position')
    )

    # 1) The line layer (no markers)
    line = chart_base.mark_line(color='gray')

    
    # 2) The point layer, with custom stroke & shape, and tooltips
    points = chart_base.mark_point(
        filled=True,         # fill the marker
        size=100,            # pixel area of each circle
        shape='circle',      # you can also pick 'triangle', 'square', etc.
        color='orange',      # fill color
        stroke='black',      # border color
        strokeWidth=2        # border thickness
    ).encode(
        tooltip=[
            alt.Tooltip('matchday:Q', title='Matchday'),
            alt.Tooltip('rolling_points:Q', title='Points'),
            alt.Tooltip('opponent_name:N', title='Opponent'),
            alt.Tooltip('date:T', title='Date'),
            alt.Tooltip('goals_scored:Q', title='Goals For'),
            alt.Tooltip('goals_conceded:Q', title='Goals Against'),
        ]
    )

    # 3) Combine and render
    chart = (line + points).interactive().properties(
        width=700,
        height=400,
        title='Season Progress with Game-by-Game Event Details'
    )

    st.altair_chart(chart, use_container_width=True)


if __name__ == "__main__":
    main()