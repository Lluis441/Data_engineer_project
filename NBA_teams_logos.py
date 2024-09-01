import sqlite3
from Functions import extract_and_load_data_teams_logos


conn = sqlite3.connect('nba_shots.db')

extract_and_load_data_teams_logos('teams_logos', conn, ['ID'],[{'column': 'id', 'references_table': 'teams', 'references_column': 'id', 'on_delete': 'CASCADE'}])

conn.close()