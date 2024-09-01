import sqlite3
from Functions import extract_and_load_data_shots


conn = sqlite3.connect('nba_shots.db')

extract_and_load_data_shots('NBA_FGA', '2023-24', conn, 'FGA',['GAME_ID', 'PLAYER_ID', 'TEAM_ID'])

extract_and_load_data_shots('NBA_FG3A', '2023-24', conn, 'FG3A',['GAME_ID', 'PLAYER_ID', 'TEAM_ID'])

conn.close()