import sqlite3
from Functions import extract_and_load_data_teams


conn = sqlite3.connect('nba_shots.db')

extract_and_load_data_teams('teams', conn, ['ID'])

conn.close()