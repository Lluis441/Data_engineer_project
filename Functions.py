#Functions to insert data into the database based on the season and the team.
from nba_api.stats.endpoints import shotchartdetail, leaguegamelog, leaguedashplayerbiostats
from nba_api.stats.static import teams
import pandas as pd
import requests

## NBA PROJECT

def create_update_table_from_dataframe(df, table_name, conn, primary_keys=None, foreign_keys=None):
    cursor = conn.cursor()

    # Check if the table exists
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
    table_exists = cursor.fetchone() is not None
    new_columns=[]

    if not table_exists:
        # Create SQL command to create table with primary keys
        columns = ', '.join([f"{col.replace(' ', '_').lower()} {get_sql_type(df[col])}" for col in df.columns])
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        """
        
        # Add PRIMARY KEY clause if primary keys are provided
        if primary_keys:
            primary_keys_str = ', '.join(primary_keys)
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
        print(f"Table '{table_name}' created.")

    else:
        # If the table exists, check for new columns and alter the table if needed
        cursor.execute(f"PRAGMA table_info({table_name});")
        existing_columns = [row[1].lower() for row in cursor.fetchall()]
        for col in df.columns:
            col_name = col.replace(' ', '_').lower()
            if col_name not in existing_columns:
                # Add new column to the table
                column_type = get_sql_type(df[col])
                alter_table_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {column_type};"
                new_columns.append(col_name)
                try:
                    cursor.execute(alter_table_sql)
                    conn.commit()
                    print(f"Column '{col_name}' added to table '{table_name}'.")
                except Exception as e:
                    print(f"Error adding column '{col_name}' to table '{table_name}': {e}")
    return new_columns

def get_sql_type(col):
    if pd.api.types.is_integer_dtype(col):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(col):
        return "REAL"
    elif pd.api.types.is_bool_dtype(col):
        return "BOOLEAN"
    else:
        return "TEXT"


def insert_data_from_dataframe(df, table_name, conn, primary_keys, new_columns):
    cursor = conn.cursor()

    # Generar la sentencia SQL para la inserción de datos
    placeholders = ', '.join(['?'] * len(df.columns))
    columns= ', '.join(df.columns)
   
    if primary_keys:
        if new_columns:
            # Construct update statement for non-primary key columns
            update_columns = ', '.join([f"{col} = excluded.{col}" for col in new_columns if col not in primary_keys])


            insert_sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({', '.join(primary_keys)})
            DO UPDATE SET {update_columns};   
            """ # Only updates when a conflict occurs with a primary key
        else:
            # If no new columns, only insert or ignore conflicts
            insert_sql = f"""
            INSERT INTO {table_name} ({columns})
            VALUES ({placeholders})
            ON CONFLICT ({', '.join(primary_keys)})
            DO NOTHING;
            """

        cursor.executemany(insert_sql, df.values.tolist())
        conn.commit()

        affected_rows = cursor.rowcount

        print(f"Attempted to insert {len(df)} rows. Inserted/Updated {affected_rows} rows into {table_name}.")
    
    else:
        insert_sql = f"""
        INSERT OR IGNORE INTO {table_name} ({columns})
        VALUES ({placeholders});
        """

        cursor.executemany(insert_sql, df.values.tolist())
        conn.commit()

        inserted_rows = cursor.rowcount

        print(f"Attempted to insert {len(df)} rows. Inserted {inserted_rows} rows into {table_name}.")
    
    



def calculate_points_per_shot(row):
    if "3" in row['SHOT_ZONE_BASIC']:
        return 3
    else:
        return 2



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

    # Add points_per_shot field based on shot_base_zone
    df['points_per_shot']=df.apply(calculate_points_per_shot, axis=1)

    # Crear la tabla basada en los campos del DataFrame
    new_columns=create_update_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn, primary_keys, new_columns)

    print(f"ETL ended: {table_name} table for the team {team_name} in the {season} season.")




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

    # Add points_per_shot field based on shot_base_zone
    #df['points_per_shot']=df.apply(calculate_points_per_shot, axis=1)

    # Crear la tabla basada en los campos del DataFrame
    new_columns=create_update_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn, primary_keys, new_columns)

    print(f"ETL ended: {table_name} table for the {season} season.")

def ETL_data_teams(table_name, conn, primary_keys=None, foreign_keys=None):
    
    # NBA API CALL
    response = teams.get_teams()

    # Convertir los resultados a un DataFrame de pandas
    df = pd.DataFrame(response)

     # Crear la tabla basada en los campos del DataFrame
    new_columns=create_update_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn, primary_keys, new_columns)

    print(f"ETL ended: {table_name} table")

def ETL_data_games(table_name, season, conn, primary_keys=None, foreign_keys=None):
    
    # NBA API CALL
    response = leaguegamelog.LeagueGameLog(season=season)

   # Convertir los resultados a un DataFrame
    df = response.get_data_frames()[0]

    # Crear la tabla basada en los campos del DataFrame
    new_columns=create_update_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn, primary_keys, new_columns)

    print(f"ETL ended: {table_name} table for the {season} season.")


def ETL_data_players_season(table_name, season, conn, primary_keys=None, foreign_keys=None):
    
    # NBA API CALL
    response = leaguedashplayerbiostats.LeagueDashPlayerBioStats(season=season)

    # Convertir los resultados a un DataFrame de pandas
    df = response.get_data_frames()[0]

    # Crear la tabla basada en los campos del DataFrame
    new_columns=create_update_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn, primary_keys, new_columns)

    print(f"ETL ended: {table_name} table")

def construct_photo_url(player_id):
    base_url = "https://cdn.nba.com/headshots/nba/latest/1040x760/"
    return f"{base_url}{player_id}.png"

def ETL_data_players_season_image(table_name, season, conn, primary_keys=None, foreign_keys=None):

    response = leaguedashplayerbiostats.LeagueDashPlayerBioStats(season=season)

    df = response.get_data_frames()[0]

    df['photo_url'] = df['PLAYER_ID'].apply(construct_photo_url)

    df=df[['PLAYER_ID','photo_url']]

    # Crear la tabla basada en los campos del DataFrame
    new_columns=create_update_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn, primary_keys, new_columns)

    print(f"ETL ended: {table_name} table")


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
    new_columns=create_update_table_from_dataframe(df, table_name , conn, primary_keys, foreign_keys)

    # Insertar los datos en la tabla
    insert_data_from_dataframe(df, table_name, conn, primary_keys, new_columns)

    print(f"ETL ended: {table_name} table")


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

            # Transform the location field from [x, y] to {'x': x_value, 'y': y_value}
            if 'location' in event and isinstance(event['location'], list) and len(event['location']) == 2:
                event['location'] = {'x': event['location'][0], 'y': event['location'][1]}
                #print(f"Transformed location: {event['location']}")
            
            # Check for nested fields like 'shot', 'pass', etc., and transform 'end_location' inside them
            nested_fields = ['shot', 'pass', 'carry'] # Checked in the Statsbomb documentation 
            for field in nested_fields:
                if field in event and 'end_location' in event[field] and isinstance(event[field]['end_location'], list):
                    # Handle end_location with either 2 or 3 elements
                    if len(event[field]['end_location']) == 2:
                        event[field]['end_location'] = {'x': event[field]['end_location'][0], 'y': event[field]['end_location'][1]}
                    elif len(event[field]['end_location']) == 3:
                        event[field]['end_location'] = {'x': event[field]['end_location'][0], 'y': event[field]['end_location'][1], 'z': event[field]['end_location'][2]}

                    #print(f"Transformed {field} end_location: {event[field]['end_location']}")


        # Get a list of all event_ids for the current match
        event_ids = [event['id'] for event in events]
        
        # Fetch existing events in bulk
        existing_event_ids = events_collection.find({'id': {'$in': event_ids}}, {'id': 1})
        existing_event_ids = {event['id'] for event in existing_event_ids}
        
        # Filter out events that are already in the database
        new_events = [event for event in events if event['id'] not in existing_event_ids]

        #print(new_events)

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