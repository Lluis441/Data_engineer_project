from pymongo import MongoClient
from Functions import *

# MongoDB setup (replace with your MongoDB connection details)
client = MongoClient("mongodb://localhost:27017/")

database_name='statsbomb_database'
matches_table_name='world_cup_2022_matches'
events_table_name='world_cup_2022_shots'

competition_id = 43 #World Cup
season_id = 106 #2022



ETL_matches_and_events_statsbomb(client,database_name,matches_table_name,events_table_name, competition_id, season_id)

# Create ETL_matches and ETL_events separatly, for if I need only one of them.