from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Literal
from sqlalchemy import create_engine, Column, Integer, String, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@localhost:5432/{DB_NAME}"


engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class Streak(BaseModel):
    streak_id: int
    streak_type: Literal['goal_streaks', 'result_streaks', 'special_streaks']
    streak_count: int
    event_name: str
    team_name: str
    league_name: str

    class Config:
        orm_mode = True


class GoalStreak(Streak):
    streak_type: Literal['goal_streaks'] = 'goal_streaks'
    

class ResultStreak(Streak):
    streak_type: Literal['result_streaks'] = 'result_streaks'

class SpecialStreak(Streak):
    streak_type: Literal['special_streaks'] = 'special_streaks'

class DBStreak(Base):
    __tablename__ = "mart_top_team_streaks"
    __table_args__ = {"schema": "raw_mart"}
    streak_id = Column(Integer, primary_key=True, index=True)
    streak_type = Column(Enum('goal_streaks', 'result_streaks', 'special_streaks', name="streak_type"), nullable=False)
    streak_count = Column('current_streak_length', Integer, nullable=False)
    event_name = Column('event_name', String, nullable=False)
    team_name = Column('team_name', String, nullable=False)
    league_name = Column('league_name', String, nullable=False)


app = FastAPI()

# Configure CORS to allow the front-end to access the API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust origin as needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get('/', response_model=List[Streak])
def get_streaks():
    """
    Get all streaks from the database.
    """
    db = SessionLocal()
    try:
        db_streaks = db.query(DBStreak).all()
        return db_streaks
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)