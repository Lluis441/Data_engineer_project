# Data_Engineer_project

## Project Overview
This project is focused on analyzing NBA game data and football data from StatsBomb using SQLite as the database backend and Tableau for data visualization. The project involves an end-to-end ETL process (Extract, Transform, Load) for both sports:

NBA Data: Shot location data, player statistics, team details, and game results are extracted from the open-source nba_api (https://github.com/swar/nba_api). The data is stored in a SQLite database and visualized in Tableau, showcasing insights on player performance, team dynamics, and shot selection strategies.

Football Data (StatsBomb): Football event data is extracted from StatsBomb’s open dataset (https://github.com/statsbomb/open-data) and stored in a MongoDB database, focusing on event-level details like shots.

The integration of these two sports allows for the analysis of shot selection and player performance in both basketball and football, providing valuable insights through comprehensive visualizations in Tableau.



## Requirements
To run this project, you'll need to have the following installed:

- Python 3.x
- SQLite 3
- MongoDB
- Tableau Public Desktop (for visualization)
- Required Python packages listed in requirements.txt

## Setup Instructions
### 1. Database Setup
Extract Data:

Use the provided Python script to extract data from the NBA API and load it into the SQLite database or exract data from Statsbomb open-data and load it into MongoDB.
Run ETL_nba_project.py to load shot location, teams, logos, players, players photos and games data from the 2023-24 season into the SQLite database.
Run ETL_WC_project.py to load matches and shots data from the 2022 football world cup into MongoDB database.
Create and Load Database:

The SQLite database (nba_project.db) is generated and populated with various NBA-related tables such as players, teams, games, and NBA_FGA (shot locations of field goals attempted) or NBA_FG3A (shot location of field goals 3 attempted). See the nba_api documentation for more info.
Ensure to set the primary keys and foreign keys as you need for proper database manipulation.

The MongoDB database is generated in your localhost with matches and shot data.


### 2. Data Analysis with Tableau
Connect to SQLite Database:

Since Tableau Public does not directly support SQLite, you can export the necessary data to CSV or Excel formats.
Example: Use the Shot_percentatge_by_zone_basic.sql to create a table and export it as a CSV file.
Then import it to Tableau Public Desktop.

### 3. Example Visualizations
Comparasion between two teams or two players or comparison between team or player vs the NBA average of shootings percentatge and points per shot, based on the shot zone.
The points per shot are a new calculated field directly in Tableau using the Tableau Calculation Language.
https://public.tableau.com/app/profile/llu.s.vega.roman/viz/ShootingPercentatgeandPointsperShoot2023-2024NBASeason/Dashboard1

## Future Work
Expand Analysis: 
Explore the nba_api and it's possibilities, with more seasons, more players... a lot of info to look and dive into.
Automate ETL Pipeline: 
Automate the extraction and loading of NBA data to keep the database up-to-date, as well as perform some more transformations to improve data quality and integration while also improving efficency and performance of the database.
Improve Visualizations: 
Explore additional Tableau visualizations (or other tools as Power BI, Qlik..) to present more complex and meaningful insights.
