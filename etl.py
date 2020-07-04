import os
import glob
import json
import psycopg2
import pandas as pd
from sql_queries import *
import datetime


def get_file_df(filepath):
    """Return file as DF"""
    dd = [json.loads(f) for f in open(filepath).readlines()]
    return pd.DataFrame(dd)
    
def process_song_file(cur, filepath):
    """
    Process song data file and inserts data to tables
        arguments:
        cur: Cursor of db
        filepath: path of the file
    """
    # open song file
    df = get_file_df(filepath)

    # insert song record
    song_data = songs_data = [df.loc[0].song_id, df.loc[0].title, df.loc[0].artist_id, int(df.loc[0].year), int(df.loc[0].duration)]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.loc[0].artist_id, df.loc[0].artist_name, df.loc[0].artist_location, df.loc[0].artist_latitude, df.loc[0].artist_longitude] 

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes log file and insert data to tables
    
    arguments:
        cur: Cursor of db
        filepath: path of the file
    """
    # open log file
    df = get_file_df(filepath)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = df['ts'].apply(lambda x: datetime.datetime.fromtimestamp(x/1000)) 
    t = df
    
    time_data = []
    for td in t['ts']:
        wd = True if td.weekday() <=6 else False
        time_data.append([str(td.time()), td.hour, td.day, td.week, td.month, td.year, wd])
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    # insert time data records
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        #user_id, level, song_id, artist_id, session_id, location, user_agent
        songplay_data = [row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Processes data, dynamically using the passed function
    
    Arguments:
        cur: db cursor
        conn: db connection
        filepath: File path
        func: function to process the data
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
    """Entry point function"""
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
        cur = conn.cursor()

        process_data(cur, conn, filepath='data/song_data', func=process_song_file)
        process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    finally:
        conn.close()


if __name__ == "__main__":
    main()