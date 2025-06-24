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
def load_latest_team_result_streaks():
    engine = get_engine()

    with engine.connect() as connection:
        query = "SELECT * FROM raw.mart_latest_team_result_streaks"
        latest_team_result_streaks_df = pd.read_sql(query, connection)
    return latest_team_result_streaks_df

def main():
    st.set_page_config(page_title="Result Streaks", layout="wide")
    st.title("⚽ Latest Result Streaks")

    with st.spinner("Loading streak data ..."):
        df = load_latest_team_result_streaks()

    #Side filters
    events = df['event_name'].unique().tolist()

    if not events:
        st.warning("No events available in the data.")
        return

    selected_event = st.sidebar.selectbox("Select Event", events, index=0)

    filtered = df[df['event_name'] == selected_event]

    st.subheader(f"Current streaks for {selected_event}")
    st.dataframe(filtered[['team_name', 'streak_count', 'date']])

if __name__ == main() :
    main()


