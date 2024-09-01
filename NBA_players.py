import sqlite3
from Functions import extract_and_load_data_players_season


conn = sqlite3.connect('nba_shots.db')
season='2023-24'

extract_and_load_data_players_season('players', season, conn, ['PLAYER_ID'])
#extract_and_load_data_players('players', conn)

conn.close()