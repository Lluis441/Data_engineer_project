#Functions to insert data into the database based on the season and the team.
from nba_api.stats.endpoints import shotchartdetail, leaguegamelog, leaguedashplayerbiostats
from nba_api.stats.static import teams
import pandas as pd
import requests

## NBA PROJECT

def create_table_from_dataframe(df, table_name, conn, primary_keys=None, foreign_keys=None):
    cursor = conn.cursor()
    
    # Create SQL command to create table with primary keys
    columns = ', '.join([f"{col.replace(' ', '_').lower()} {get_sql_type(df[col])}" for col in df.columns])
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    """
    
    # Add PRIMARY KEY clause if primary keys are provided
    if primary_keys:
        primary_keys_str = ', '.join([key.replace(' ', '_').lower() for key in primary_keys])
        create_table_sql += f""",
        PRIMARY KEY ({primary_keys_str})
        """
    
    # Add FOREIGN KEY clauses if foreign keys are provided
    if foreign_keys:
        for fk in foreign_keys:
            create_table_sql += f""",
            FOREIGN KEY ({fk['column'].replace(' ', '_').lower()})
            REFERENCES {fk['references_table']}({fk['references_column'].replace(' ', '_').lower()})
            ON DELETE {fk.get('on_delete', 'NO ACTION')}
            """
    
    create_table_sql += ");"

    cursor.execute(create_table_sql)
    conn.commit()

def get_sql_type(col):
    if pd.api.types.is_integer_dtype(col):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(col):
        return "REAL"
    elif pd.api.types.is_bool_dtype(col):
        return "BOOLEAN"
    else:
        return "TEXT"


def insert_data_from_dataframe(df, table_name, conn):
    cursor = conn.cursor()
    # Generar la sentencia SQL para la inserción de datos
    placeholders = ', '.join(['?'] * len(df.columns))
    columns= ', '.join(df.columns)
    insert_sql = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"

    cursor.executemany(insert_sql, df.values.tolist())
    conn.commit()


def ETL_data_team_shots(team_name, table_name, season, conn, context_measure, primary_keys=None, foreign_keys=None):
    # Obtener el ID del equipo
    team_dict = teams.find_teams_by_full_name(team_name)[0]
    team_id = team_dict['id']
    
    # Llamar a la API de NBA para obtener los detalles de los tiros
    response = shotchartdetail.ShotChartDetail(
        team_id=team_id,
        player_id=0,  # 0 para todos los jugadores
        season_nullable=season,
        context_measure_simple=context_measure
    )

    # Convertir los resultados a un DataFrame de pandas
    df = response.get_data_frames()[0]

    # Crear la tabla basada en los campos del DataFrame
    create_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn)

    print(f"Data has been inserted into the {table_name} table for the team {team_name} in the {season} season.")


def ETL_data_shots(table_name, season, conn, context_measure, primary_keys=None, foreign_keys=None):
    
    # NBA API CALL
    response = shotchartdetail.ShotChartDetail(
        team_id=0, # 0 all teams
        player_id=0,  # 0 all players
        season_nullable=season,
        context_measure_simple=context_measure
    )

    # Convertir los resultados a un DataFrame de pandas
    df = response.get_data_frames()[0]

    # Crear la tabla basada en los campos del DataFrame
    create_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn)

    print(f"Data has been inserted into the {table_name} table for the {season} season.")

def ETL_data_teams(table_name, conn, primary_keys=None, foreign_keys=None):
    
    # NBA API CALL
    response = teams.get_teams()

    # Convertir los resultados a un DataFrame de pandas
    df = pd.DataFrame(response)

    # Crear la tabla basada en los campos del DataFrame
    create_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn)

    print(f"Data has been inserted into the {table_name} table")

def ETL_data_games(table_name, season, conn, primary_keys=None, foreign_keys=None):
    
    # NBA API CALL
    response = leaguegamelog.LeagueGameLog(season=season)

   # Convertir los resultados a un DataFrame
    df = response.get_data_frames()[0]

    # Crear la tabla basada en los campos del DataFrame
    create_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn)

    print(f"Data has been inserted into the {table_name} table for the {season} season.")


def ETL_data_players_season(table_name, season, conn, primary_keys=None, foreign_keys=None):
    
    # NBA API CALL
    response = leaguedashplayerbiostats.LeagueDashPlayerBioStats(season=season)

    # Convertir los resultados a un DataFrame de pandas
    df = response.get_data_frames()[0]

    # Crear la tabla basada en los campos del DataFrame
    create_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn)

    print(f"Data has been inserted into the {table_name} table")

def construct_photo_url(player_id):
    base_url = "https://cdn.nba.com/headshots/nba/latest/1040x760/"
    return f"{base_url}{player_id}.png"

def ETL_data_players_season_image(table_name, season, conn, primary_keys=None, foreign_keys=None):

    response = leaguedashplayerbiostats.LeagueDashPlayerBioStats(season=season)

    df = response.get_data_frames()[0]

    df['photo_url'] = df['PLAYER_ID'].apply(construct_photo_url)

    df=df[['PLAYER_ID','photo_url']]

     # Crear la tabla basada en los campos del DataFrame
    create_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn)

    print(f"Data has been inserted into the {table_name} table")


def construct_logo_url(team_id, logo_type='primary', size='L'):
    base_url = "https://cdn.nba.com/logos/nba/"
    return f"{base_url}{team_id}/{logo_type}/{size}/logo.svg"

def ETL_data_teams_logos(table_name, conn, primary_keys=None, foreign_keys=None):

    response = teams.get_teams()

    df = pd.DataFrame(response)

    df['primary_logo_url'] = df['id'].apply(lambda team_id: construct_logo_url(team_id, 'primary'))
    df['secondary_logo_url'] = df['id'].apply(lambda team_id: construct_logo_url(team_id, 'secondary')) #not accessible url
    df['alt_logo_url'] = df['id'].apply(lambda team_id: construct_logo_url(team_id, 'alt')) #not accessible url

    df=df[['id','primary_logo_url','secondary_logo_url', 'alt_logo_url']]

     # Crear la tabla basada en los campos del DataFrame
    create_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn)

    print(f"Data has been inserted into the {table_name} table")


## WC PROJECT

def download_events_and_filter(match_id, event_type):
    events_url = f'https://raw.githubusercontent.com/statsbomb/open-data/master/data/events/{match_id}.json'
    response = requests.get(events_url)
    
    if response.status_code == 200:
        events_data = response.json()
        
        # Filter for only the specified event type
        events_type = [event for event in events_data if event['type']['name'] == event_type]
        return events_type
    else:
        print(f"Failed to download events for match {match_id}. Status code: {response.status_code}")
        return []

def process_events_for_match(match_id, event_type, events_collection):
    events = download_events_and_filter(match_id, event_type)
    
    if events:
        # Add the match_id to each event
        for event in events:
            event['match_id'] = match_id

        # Get a list of all event_ids for the current match
        event_ids = [event['id'] for event in events]
        
        # Fetch existing events in bulk
        existing_event_ids = events_collection.find({'id': {'$in': event_ids}}, {'id': 1})
        existing_event_ids = {event['id'] for event in existing_event_ids}
        
        # Filter out events that are already in the database
        new_events = [event for event in events if event['id'] not in existing_event_ids]

        # Insert new events in bulk
        if new_events:
            events_collection.insert_many(new_events)
            print(f"Inserted {len(new_events)} new {event_type} events for match {match_id} into MongoDB.")
        else:
            print(f"No new {event_type} events for match {match_id}.")
    else:
        print(f"No {event_type} events found for match {match_id}.")

def ETL_matches_and_event_statsbomb(client, database_name, matches_table_name, events_table_name, event_type, competition_id, season_id, match_id=None):

    db = client[database_name]
    matches_collection = db[matches_table_name]
    events_collection = db[events_table_name]

    # Download World Cup 2022 matches JSON data directly
    matches_url = f'https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/{competition_id}/{season_id}.json'
    
    response = requests.get(matches_url)

    if response.status_code == 200:
        matches_data = response.json()

        if match_id is not None:
            # Check if the specific match already exists in the database
            existing_match = matches_collection.find_one({'match_id': match_id})
            
            if existing_match:
                print(f"Match {match_id} already exists in MongoDB, skipping insertion in {matches_table_name}")

                process_events_for_match(match_id, event_type, events_collection)
            else:
                # Filter for the specific match
                filtered_match = next((match for match in matches_data if match['match_id'] == match_id), None)
                if filtered_match:
                    matches_collection.insert_one(filtered_match)
                    print(f"Inserted match {match_id} into MongoDB.")

                # Process events for the specific match
                process_events_for_match(match_id, event_type, events_collection)

        else:
            # Extract all match_ids from the downloaded matches
            match_ids = [match['match_id'] for match in matches_data]

            # Find all matches that already exist in the database
            existing_matches = matches_collection.find({'match_id': {'$in': match_ids}}, {'match_id': 1})

            # Get the match_ids of existing matches
            existing_match_ids = {match['match_id'] for match in existing_matches}

            # Filter out matches that are already in the database
            new_matches = [match for match in matches_data if match['match_id'] not in existing_match_ids]

            if new_matches:
                # Insert all new matches in a single bulk operation
                matches_collection.insert_many(new_matches)
                print(f"Inserted {len(new_matches)} new matches into MongoDB.")
            else:
                print("No new matches to insert.")
                return
            
            # Process events for each match
            for match_id in [match['match_id'] for match in new_matches]:
                process_events_for_match(match_id, event_type, events_collection)

    else:
        print(f"Failed to download matches. Status code: {response.status_code}")


'''
def ETL_event_statsbomb(client,database_name,events_table_name, match_id, event_type):

    db = client[database_name]
    events_collection = db[events_table_name]

    events = download_events_and_filter(match_id,event_type)

    if events:
        # Get a list of all event_ids for the current match
        event_ids = [event['id'] for event in events]
        
        # Fetch existing events in bulk
        existing_event_ids = events_collection.find({'id': {'$in': event_ids}}, {'id': 1})
        existing_event_ids = {event['id'] for event in existing_event_ids}
        
        # Filter out events that are already in the database
        new_events = [event for event in events if event['id'] not in existing_event_ids]

        # Insert new events in bulk
        if new_events:
            events_collection.insert_many(new_events)
            print(f"Inserted {len(new_events)} new {event_type} events for match {match_id} into MongoDB.")
        else:
            print(f"No new {event_type} events for match {match_id}.")
    else:
        print(f"No {event_type} events found for match {match_id}.")



def ETL_matches_statsbomb(client, database_name, matches_table_name, competition_id, season_id):

    db = client[database_name]
    matches_collection = db[matches_table_name]

    # Download World Cup 2022 matches JSON data directly
    matches_url = f'https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/{competition_id}/{season_id}.json'

    response = requests.get(matches_url)
    if response.status_code == 200:
        matches_data = response.json()  # This is the raw JSON data

        # Extract all match_ids from the downloaded matches
        match_ids = [match['match_id'] for match in matches_data]

        # Find all matches that already exist in the database
        existing_matches = matches_collection.find({'match_id': {'$in': match_ids}}, {'match_id': 1})

        # Get the match_ids of existing matches
        existing_match_ids = {match['match_id'] for match in existing_matches}

        # Filter out matches that are already in the database
        new_matches = [match for match in matches_data if match['match_id'] not in existing_match_ids]

        if new_matches:
            # Insert all new matches in a single bulk operation
            matches_collection.insert_many(new_matches)
            print(f"Inserted {len(new_matches)} new matches into MongoDB.")
        else:
            print("No new matches to insert.")

    else:
        print(f"Failed to download matches. Status code: {response.status_code}")

'''