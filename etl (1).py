import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process the song files
    Step 1: read the song files
    step 2: select just the necessery coluns to insert into songs table
    stpe 3: sele just the necessery coluns to insert into artists table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title','artist_id', 'year', 'duration']].values)[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values)[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process the log files
    Step 1: read the song files
    step 2: filter the read df to onlye "NextSong" records
    stpe 3: convert timestamp column to datetime, and create the hour, day, week, month. year and weekday columns based on datetime column
    step 4: insert the data records
    step 5: select only the necessary colunms for user table and insert them.
    step 6: insert songplay records
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.loc[df["page"] == 'NextSong']

    # convert timestamp column to datetime
    
    t = df
    t['time_data'] = pd.to_datetime(df['ts'], unit='ms', errors='coerce')
    t['timestamp'] = t['time_data']
    t['timestamp'] = t['time_data'].apply(lambda x : None if x=="NaN" else x)
    t['hour'] = t['time_data'].dt.hour
    t['day'] = t['time_data'].dt.day
    t['week'] = t['time_data'].dt.week
    t['month'] = t['time_data'].dt.month
    t['year'] = t['time_data'].dt.year
    t['weekday'] = t['time_data'].dt.weekday
    t = t[['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']]
    
    # insert time data records
#     time_data = 
#     column_labels = 
    time_df = t[['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    df['start_time'] = pd.to_datetime(df['ts'], unit='ms')
    
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
        aux = row[['start_time', 'userId', 'level','song','artist', 'sessionId', 'location', 'userAgent']].values
        aux[4] = artistid
        aux[3] = songid
        
        # insert songplay record
        songplay_data = aux
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Function used to read all the files in a root path
    filter for json files format
    and use the paths returned in the function from the function parameter
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ 
    main function, used to connect in the database and call process_data funtion
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()