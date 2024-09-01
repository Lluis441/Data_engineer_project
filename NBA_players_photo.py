import sqlite3
from Functions import extract_and_load_data_players_season_image 


conn = sqlite3.connect('nba_shots.db')
season='2023-24'

extract_and_load_data_players_season_image('players_photo', season, conn, ['PLAYER_ID'],[{'column': 'player_id', 'references_table': 'players', 'references_column': 'player_id', 'on_delete': 'CASCADE'}])
#extract_and_load_data_players('players', conn)

conn.close()