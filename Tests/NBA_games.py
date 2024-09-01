import sqlite3
from Functions import extract_and_load_data_games


conn = sqlite3.connect('nba_shots.db')

extract_and_load_data_games('games', '2023-24', conn, ['GAME_ID'])

conn.close()
