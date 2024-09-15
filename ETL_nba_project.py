import sqlite3
from Functions import *

conn = sqlite3.connect('/Users/lluis44/data-engineering-db/nba_project.db')
season='2023-24'

ETL_data_shots('NBA_FGA', '2023-24', conn, 'FGA', ['player_id', 'team_id', 'period', 'minutes_remaining', 'seconds_remaining', 'loc_x', 'loc_y'])

ETL_data_shots('NBA_FG3A', '2023-24', conn, 'FG3A', ['player_id', 'team_id', 'period', 'minutes_remaining', 'seconds_remaining', 'loc_x', 'loc_y'])

ETL_data_teams('teams', conn, ['id'])

ETL_data_teams_logos('teams_logos', conn, ['id'],[{'column': 'id', 'references_table': 'teams', 'references_column': 'id', 'on_delete': 'CASCADE'}])

ETL_data_players_season('players', season, conn, ['player_id'])

ETL_data_players_season_image('players_photo', season, conn, ['player_id'],[{'column': 'player_id', 'references_table': 'players', 'references_column': 'player_id', 'on_delete': 'CASCADE'}])

ETL_data_games('games', '2023-24', conn, ['game_id','team_id'])

conn.close()