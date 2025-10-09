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
                        position,
                        team_name,
                        games_played,
                        rolling_points AS points,
                        wins,
                        losses,
                        draws,
                        form,
                        goals_for,
                        goals_against
                    FROM raw.mart_teams_standing'''
        
        latest_team_standing_df = pd.read_sql(query, connection)
    return latest_team_standing_df

def main():
    st.set_page_config(page_title="Dashboard", layout='wide')
    st.title("Dashboard Analysis")

    with st.spinner("Loading Analysis....."):
        df = load_latest_team_standing()
    
    leagues = sorted(df["league"].unique())

    selected_league = st.selectbox("Select League", leagues)

    season_df = df.query("league == @selected_league").copy()
    
    # 7) Drill‐down into a team
    st.subheader("🔍 Drill into a Team")
    team = st.selectbox("Select a team to explore", season_df.team_name)
    team_url = f"./team_page?team={team.replace(' ', '%20')}"
    st.markdown(
        f'<a href="{team_url}" target="_self">'
        f'<button style="background-color:#4CAF50;color:white;padding:10px 24px;border:none;cursor:pointer;">'
        f'Go to Team Detail</button></a>',
        unsafe_allow_html=True
    )

    season_df["ppg"] = season_df.points / season_df.games_played
    season_df["gd"] = season_df.goals_for - season_df.goals_against

    top_ppg = season_df.loc[season_df.ppg.idxmax()]
    top_gd = season_df.loc[season_df.gd.idxmax()]
    top_gf = season_df.loc[season_df.goals_for.idxmax()]
    best_def = season_df.loc[season_df.goals_against.idxmin()]
    avg_gpm = season_df.goals_for.sum() / season_df.games_played.sum()
    #longest_unbeaten = season_df.unbeaten_strea
    #st.dataframe(df)

    st.subheader(f"🏆 Season KPIs — {selected_league}")
    c1, c2, c3 = st.columns(3)
    c1.metric("Top PPG",        f"{top_ppg.ppg:.2f}",          top_ppg.team_name)
    c2.metric("Best GD",        f"{top_gd.gd}",                top_gd.team_name)
    c3.metric("Most Goals For", f"{top_gf.goals_for}",         top_gf.team_name)

    c4, c5 = st.columns(2)
    c4.metric("Best Defense",   f"{best_def.goals_against}",   best_def.team_name)
    c5.metric("League Avg G/Match", f"{avg_gpm:.2f}")

    # 4) Standings Table
    st.subheader("📋 Current Standings")
    table = season_df.set_index("position")[[
        "team_name","points","games_played","wins","draws","losses","goals_for","goals_against","gd"
    ]]
    st.dataframe(table, use_container_width=True)

    # 5) Goals For vs Against Scatter
    st.subheader("⚽️ Goals For vs Goals Against")
    scatter_chart = (
        alt.Chart(season_df)
        .mark_circle(size=70)
        .encode(
            x=alt.X('goals_for:Q', title='Goals scored'),
            y=alt.Y('goals_against:Q', title='Goals Conceded'),
            color=alt.Color(
                'points:Q',
                title='Points',
                scale=alt.Scale(scheme='oranges')
            ),
            size=alt.Size('points:Q',
                          scale=alt.Scale(range=[30, 200]),
                          legend=alt.Legend(title='Points')),
            tooltip=[
                alt.Tooltip('team_name:N', title='Team'),
                alt.Tooltip('points:Q', title='Points'),
                alt.Tooltip('goals_for:Q', title='Goals For'),
                alt.Tooltip('goals_against:Q', title='Goals Against'),
                alt.Tooltip('position:O',  title='Position')
            ]
        )
        .interactive()
        .properties(
            width=600,
            height=400,
            title='Goals For vs Goals Against (size by points)'
        )
    )

    st.altair_chart(scatter_chart, use_container_width=True)

    # 6) Distribution of Goals Scored
    st.subheader("📈 Distribution of Season Goals Scored")
    hist = season_df["goals_for"].value_counts().sort_index()
    st.bar_chart(hist)



if __name__ == "__main__":
    main()