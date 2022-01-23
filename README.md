# Purpose
The purpose of this database is to carry out analyzes on the platform's streams, since the fact table concentrates information about music performances, being able to generate analyzes by song, artist, region or subscription level.

# How to run
to run this project, need to follow this steps:
1. Open the Console
2. run this commands: 

> python create_tables.py
> python etl.py

# Database design
![This is an image](https://github.com/trickmartini/data-modeling-with-postgres/blob/95b6f6d354c8ab5ece6f838960cf71650cc3a637/ER%20Diagram.png)


user table: contains user information as first and last name, gender and level;
songs table: contains songs informations like title, artist_id, year and duration;
artist table: contians artists informations like name, location, latitude and longitude;
songPlays: contains information about the streams plays, like start_time of streamming, user_id, level, song_id, artist_id, session_id, location and user_agent;
time table: contains information about the streams time, like start_time, hour, day, week, month, year, weekday. 

# Etl Process
## Process the log files
    Step 1: read the song files
    step 2: filter the read df to onlye "NextSong" records
    stpe 3: convert timestamp column to datetime, and create the hour, day, week, month. year and weekday columns based on datetime column
    step 4: insert the data records
    step 5: select only the necessary colunms for user table and insert them.
    step 6: insert songplay records

## Process the song files
    Step 1: read the song files
    step 2: select just the necessery coluns to insert into songs table
    stpe 3: sele just the necessery coluns to insert into artists table

# Project Structure
1.data folder: contains the data files (logs and songs)
2.create_tables.py : python code that execute the queries from sql_queries.py file to create the database and tables if they don't exists
3.etl.ipynb: jupyter notebook used to build and test the etl functions
4.etl.py: python file with functions used to execute the ETL process. 
5.sql_queries.py: a python file with the  DROP, CREATE and INSERT statements, used by the functions on create_tables.py functions.
6.test.ipynb: jupyter notebook used to test if the etl functions worked right.

# Justification
The database schema has one fact and four dimensions, with four of these tables capable of crossing information while the time dimension does not have the keys needed to perform joins with other tables, being used to generate metrics of runtimes of streams. for periods of time.

The ETL process reads the jsons files present in the **data** folder, selects the necessary columns for each table and inserts them into the database.


# queryes exemple:
## get quantity of streams by location and level

> SELECT location, level, count(*) qty_executions FROM songplays group by 1,2 order by 1

## qty of streams by level

> SELECT level, count(*) qty_executions FROM songplays group by 1 order by 1

## region with more artists registered in the platform

> SELECT location, count(*)  FROM artists group by 1 order by 2 DESC

