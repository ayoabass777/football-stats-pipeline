import streamlit as st
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
def load_latest_team_goal_streaks():
    engine = get_engine()
    
    with engine.connect() as connection:
        query = "SELECT * FROM raw.mart_latest_team_goal_streaks"
        latest_team_goal_streaks_df = pd.read_sql(query, connection)
    return latest_team_goal_streaks_df

def main():
    st.set_page_config(page_title="Team Streaks", layout="wide")
    st.title("⚽ Latest Goal Streaks")

    with st.spinner("Loading streak data...."):
        df = load_latest_team_goal_streaks()

    # Sidebar filters
    events = df['event_name'].unique().tolist()
    if not events:
        st.warning("No events available in the data.")
        return
    selected_event = st.sidebar.selectbox("Select Event", events, index=0)

    # Filtered DataFrame
    filtered = df[df['event_name'] == selected_event]

    #Show the table
    st.subheader(f"Current streaks for {selected_event}")
    st.dataframe(filtered[['team_name', 'streak_count', 'date']])

    #Bar chart of top streaks
    top_n = st.sidebar.slider("Show top N teams", min_value=5, max_value=20, value=10)
    chart_data = filtered.nlargest(top_n, 'streak_count').set_index('team_name')['streak_count']
    st.bar_chart(chart_data)

if __name__ == "__main__":
    main()