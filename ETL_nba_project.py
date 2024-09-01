import sqlite3
from Functions import *

conn = sqlite3.connect('nba_project.db')
season='2023-24'

ETL_data_shots('NBA_FGA', '2023-24', conn, 'FGA')

ETL_data_shots('NBA_FG3A', '2023-24', conn, 'FG3A')

ETL_data_teams('teams', conn, ['ID'])

ETL_data_teams_logos('teams_logos', conn, ['ID'],[{'column': 'id', 'references_table': 'teams', 'references_column': 'id', 'on_delete': 'CASCADE'}])

ETL_data_players_season('players', season, conn, ['PLAYER_ID'])

ETL_data_players_season_image('players_photo', season, conn, ['PLAYER_ID'],[{'column': 'player_id', 'references_table': 'players', 'references_column': 'player_id', 'on_delete': 'CASCADE'}])

ETL_data_games('games', '2023-24', conn, ['GAME_ID'])

conn.close()