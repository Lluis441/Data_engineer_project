import sqlite3
from Functions import extract_and_load_data_team_shots


conn = sqlite3.connect('nba_shots.db')

extract_and_load_data_team_shots('Los Angeles Lakers','Lakers_FGA', '2023-24', conn, 'FGA', ['GAME_ID', 'PLAYER_ID', 'TEAM_ID'])

extract_and_load_data_team_shots('Los Angeles Lakers','Lakers_FG3A_shots', '2023-24', conn, 'FG3A', ['GAME_ID', 'PLAYER_ID', 'TEAM_ID'])

conn.close()


